/*
 * Copyright 2018-2023 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "create_table.h"

#include <any>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <boost/assert.hpp>

#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/conv/create_default_value.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/validate_index_key_type.h>

namespace jogasaki::executor::common {

using takatori::util::unsafe_downcast;
using takatori::util::string_builder;

create_table::create_table(
    takatori::statement::create_table& ct
) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind create_table::kind() const noexcept {
    return model::statement_kind::create_table;
}

bool create_sequence_for_generated_pk(
    request_context& context,
    yugawara::storage::sequence& p
) {
    executor::sequence::metadata_store ms{*context.transaction()->object()};
    std::size_t def_id{};
    try {
        ms.find_next_empty_def_id(def_id);
        p.definition_id(def_id); // TODO p is part of prepared statement, avoid updating it directly
        context.sequence_manager()->register_sequence(
            std::addressof(static_cast<kvs::transaction &>(*context.transaction())),
            *p.definition_id(),
            p.simple_name(),
            p.initial_value(),
            p.increment_value(),
            p.min_value(),
            p.max_value(),
            p.cycle(),
            true
        );
//        provider.add_sequence(p);  // sequence definition is added in serializer, no need to add it here
    } catch (sequence::exception& e) {
        handle_kvs_errors(context, e.get_status());
        handle_generic_error(context, e.get_status(), error_code::sql_execution_exception);
        return false;
    }
    return true;
}

bool validate_type(
    request_context& context,
    std::string_view colname,
    takatori::type::decimal const& typ
) {
    std::string_view reason{};
    if(! typ.scale()) {
        reason = "invalid scale";
    } else if(typ.precision() && ! (typ.precision().value() >= decimal_min_precision && typ.precision().value() <= decimal_max_precision)) {
        reason = "invalid precision";
    } else if(typ.precision() && typ.scale() && ! (typ.scale().value() <= typ.precision().value())) {
        reason = "scale out of range for the precision";
    } else {
        return true;
    }
    set_error(
        context,
        error_code::unsupported_runtime_feature_exception,
        string_builder{} << "decimal type on column \"" << colname << "\" is unsupported (" << reason << ")" << string_builder::to_string,
        status::err_unsupported
    );
    return false;
}

bool validate_type(
    request_context& context,
    std::string_view colname,
    takatori::type::character const& typ
) {
    std::string_view reason{};
    if(typ.length() && !(typ.length().value() >= 1 && typ.length().value() <= 30716)) {
        reason = "invalid length";
    } else {
        return true;
    }
    set_error(
        context,
        error_code::unsupported_runtime_feature_exception,
        string_builder{} << "character type on column \"" << colname << "\" is unsupported (" << reason << ")" << string_builder::to_string,
        status::err_unsupported
    );
    return false;
}

bool validate_type(
    request_context& context,
    std::string_view colname,
    takatori::type::octet const& typ
) {
    std::string_view reason{};
    if(typ.length() && !(typ.length().value() >= 1 && typ.length().value() <= 30716)) {
        reason = "invalid length";
    } else {
        return true;
    }
    set_error(
        context,
        error_code::unsupported_runtime_feature_exception,
        string_builder{} << "octet type on column \"" << colname << "\" is unsupported (" << reason << ")" << string_builder::to_string,
        status::err_unsupported
    );
    return false;
}

bool validate_default_value(
    request_context& context,
    yugawara::storage::column const& c
) {
    if(c.default_value().kind() == yugawara::storage::column_value_kind::immediate) {
        auto& dv = c.default_value().element<yugawara::storage::column_value_kind::immediate>();
        if(auto a = conv::create_immediate_default_value(*dv, c.type(), context.request_resource()); a.error()) {
            set_error(
                context,
                error_code::unsupported_runtime_feature_exception,
                string_builder{} << "unable to convert default value for column \"" << c.simple_name()
                                 << "\" to type " << c.type() << string_builder::to_string,
                status::err_unsupported
            );
            return false;
        }
    }
    return true;
}

bool validate_table_definition(
    request_context& context,
    yugawara::storage::table const& t
) {
    using takatori::type::type_kind;
    for(auto&& c : t.columns()) {
        if(! validate_default_value(context, c)) {
            return false;
        }
        switch(c.type().kind()) {
            case type_kind::decimal:
                if(! validate_type(context, c.simple_name(), unsafe_downcast<takatori::type::decimal const>(c.type()))) {
                    return false;
                }
                continue;
            case type_kind::character:
                if(! validate_type(context, c.simple_name(), unsafe_downcast<takatori::type::character const>(c.type()))) {
                    return false;
                }
                continue;
            case type_kind::int4:
            case type_kind::int8:
            case type_kind::float4:
            case type_kind::float8:
            case type_kind::date:
            case type_kind::time_of_day:
            case type_kind::time_point:
                continue;
            case type_kind::octet:
                if(context.configuration()->support_octet()) {
                    if(! validate_type(
                           context,
                           c.simple_name(),
                           unsafe_downcast<takatori::type::octet const>(c.type())
                       )) {
                        return false;
                    }
                    continue;
                }
                break;
            case type_kind::boolean:
            case type_kind::int1:
            case type_kind::int2:
                if(context.configuration()->support_smallint()) {
                    continue;
                }
                break;
            default:
                break;
        }
        set_error(
            context,
            error_code::unsupported_runtime_feature_exception,
            string_builder{} << "Data type specified for column \"" << c.simple_name() << "\" is unsupported."
                             << string_builder::to_string,
            status::err_unsupported
        );
        return false;
    }
    return true;
}

bool create_table::operator()(request_context& context) const {
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    auto& provider = *context.storage_provider();
    auto c = yugawara::binding::extract_shared<yugawara::storage::table>(ct_->definition());
    if(provider.find_table(c->simple_name())) {
        set_error(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Table \"" << c->simple_name() << "\" already exists." << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }
    if(! validate_table_definition(context, *c)) {
        return false;
    }

    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->primary_key());
    if(! utils::validate_index_key_type(context, *i)) {
        return false;
    }

    // Creating sequence can possibly hit cc engine error (esp. with occ),
    // so do it first so that we can exit early in case of errors.
    auto rh = std::any_cast<plan::storage_processor_result>(ct_->runtime_hint());
    if(rh.primary_key_generated()) {
        auto p = rh.primary_key_sequence();
        if(!create_sequence_for_generated_pk(context, *p)) {
            return false;
        }
    }

    std::string storage{};
    yugawara::storage::configurable_provider target{};
    if(auto err = recovery::create_storage_option(*i, storage, utils::metadata_serializer_option{false})) {
        // error should not happen normally
        set_error_info(context, err);
        return false;
    }
    if(auto err = recovery::deserialize_storage_option_into_provider(storage, provider, provider, true)) {
        // error should not happen normally
        // validating version failure does not happen as serialization is just done above.
        set_error_info(context, err);
        return false;
    }

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage(c->simple_name(), options);! stg) {
        // should not happen normally
        set_error(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Storage \"" << c->simple_name() << "\" already exists " << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }
    return true;
}

}
