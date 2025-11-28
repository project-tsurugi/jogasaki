/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/surrogate_id_utils.h>
#include <jogasaki/utils/validate_index_key_type.h>
#include <jogasaki/utils/validate_table_definition.h>

namespace jogasaki::executor::common {

using takatori::util::string_builder;

create_table::create_table(
    takatori::statement::create_table& ct
) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind create_table::kind() const noexcept {
    return model::statement_kind::create_table;
}

static bool create_generated_sequence(
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
            true,
            true // create new seq_id
        );
//        provider.add_sequence(p);  // sequence definition is added in serializer, no need to add it here
    } catch (sequence::exception& e) {
        handle_kvs_errors(context, e.get_status());
        handle_generic_error(context, e.get_status(), error_code::sql_execution_exception);
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

    // currently no schema is supported, so only admin can create table
    if (auto& s = context.req_info().request_source()) {
        if(s->session_info().user_type() != tateyama::api::server::user_type::administrator) {
            auto username = s->session_info().username();
            VLOG_LP(log_error) << "insufficient authorization user:\""
                               << (username.has_value() ? username.value() : "")
                               << "\"";
            set_error(
                context,
                error_code::permission_error,
                "insufficient authorization for the requested operation",
                status::err_illegal_operation
            );
            return false;
        }
    }

    if(! utils::validate_table_definition(context, *c)) {
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
        if(! create_generated_sequence(context, *p)) {
            return false;
        }
    }
    for(auto&& s : rh.generated_sequences()) {
        if(! create_generated_sequence(context, *s)) {
            return false;
        }
    }

    auto tid = storage::index_id_src++;
    auto& smgr = *global::storage_manager();

    // note: this code generating surrogate id has been added in release 1.8,
    // and existing tables/indices that were created before the release
    // do not have surrogate IDs
    auto storage_key = utils::to_big_endian(smgr.generate_surrogate_id());
    auto opt = global::config_pool()->enable_storage_key() ? std::optional<std::string_view>{storage_key} : std::nullopt;

    if(! smgr.add_entry(tid, c->simple_name(), opt, true)) {
        // should not happen normally
        set_error(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Table id:" << tid << " already exists" << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }

    auto se = smgr.find_entry(tid);
    if(! se) {
        // should not happen normally
        set_error(
            context,
            error_code::sql_execution_exception,
            string_builder{} << "Table id:" << tid << " not found" << string_builder::to_string,
            status::err_unknown
        );
        return false;
    }

    // the creator owns the CONTROL privilege on the newly created table
    if (context.req_info().request_source()) {
        if(auto name = context.req_info().request_source()->session_info().username(); name.has_value()) {
            se->authorized_actions().add_user_actions(name.value(), auth::action_set{auth::action_kind::control});
        }
    }

    std::string storage{};
    yugawara::storage::configurable_provider target{};

    if(auto err = recovery::create_storage_option(
           *i,
           storage,
           utils::metadata_serializer_option{
               false,
               std::addressof(se->authorized_actions()),
               nullptr,
               opt
           }
       )) {
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
    if(auto stg = context.database()->create_storage((opt.has_value() ? opt.value() : c->simple_name()), options);! stg) {
        // should not happen normally
        set_error(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Storage \"" << c->simple_name() << "\" already exists " << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }
    auto& tx = *context.transaction();
    if(! tx.storage_lock()) {
        tx.storage_lock(smgr.create_unique_lock());
    }
    storage::storage_list stg{tid};
    if(! smgr.add_locked_storages(stg, *tx.storage_lock())) {
        // should not happen normally since this is newly created table
        set_error(
            context,
            error_code::sql_execution_exception,
            "DDL operation was blocked by other DML operation",
            status::err_illegal_operation
        );
        return false;
    }
    return true;
}

}
