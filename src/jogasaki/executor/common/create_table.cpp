/*
 * Copyright 2018-2020 tsurugi project.
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

#include <yugawara/binding/extract.h>
#include <takatori/util/fail.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/handle_generic_error.h>

#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/storage_options.h>

namespace jogasaki::executor::common {

using takatori::util::fail;
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

bool validate_table_definition(
    request_context& context,
    yugawara::storage::table const& t
) {
    using takatori::type::type_kind;
    for(auto&& c : t.columns()) {
        switch(c.type().kind()) {
            case type_kind::boolean:
            case type_kind::int1:
            case type_kind::int2:
            case type_kind::int4:
            case type_kind::int8:
            case type_kind::float4:
            case type_kind::float8:
            case type_kind::character:
            case type_kind::date:
            case type_kind::time_of_day:
            case type_kind::time_point:
            case type_kind::decimal:
                break;
            default:
                set_error(
                    context,
                    error_code::unsupported_runtime_feature_exception,
                    string_builder{} << "Data type specified for column \"" << c.simple_name() << "\" is unsupported." << string_builder::to_string,
                    status::err_unsupported
                );
                return false;
        }
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
    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->primary_key());
    if(! recovery::create_storage_option(*i, storage, utils::metadata_serializer_option{false})) {
        // error should not happen normally
        set_error(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Table already exists." << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }
    if(! recovery::deserialize_storage_option_into_provider(storage, provider, provider, true)) {
        // error should not happen normally
        // validating version failure does not happen as serialization is just done above.
        set_error(
            context,
            error_code::sql_execution_exception,
            "Unexpected error occurred.",
            status::err_unknown
        );
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
