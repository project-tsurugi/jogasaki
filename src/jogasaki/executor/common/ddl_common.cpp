/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "ddl_common.h"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <glog/logging.h>

#include <takatori/util/string_builder.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_list.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/append_request_info.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/surrogate_id_utils.h>

namespace jogasaki::executor::common {

using takatori::util::string_builder;

bool create_generated_sequence(
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
    } catch (sequence::exception& e) {
        handle_kvs_errors(context, e.get_status());
        handle_generic_error(context, e.get_status(), error_code::sql_execution_exception);
        return false;
    }
    return true;
}

bool remove_generated_sequence(
    request_context& context,
    std::string_view sequence_name,
    yugawara::storage::sequence const& s
) {
    if(! s.definition_id().has_value()) {
        return true;
    }
    auto def_id = s.definition_id().value();
    try {
        if(! context.sequence_manager()->remove_sequence(def_id, context.transaction()->object().get())) {
            // even on error, continue clean-up as much as possible
            VLOG_LP(log_info) << "sequence '" << sequence_name << "' not found";
        }
    } catch(executor::sequence::exception const& e) {
        // unrecoverable error - transaction aborted and we cannot continue
        VLOG_LP(log_error) << "removing sequence '" << sequence_name << "' failed";
        set_error_context(
            context,
            error_code::sql_execution_exception,
            e.what(),
            e.get_status()
        );
        return false;
    }
    return true;
}

bool reserve_delete_index_metadata(
    request_context& context,
    std::string_view index_name,
    yugawara::storage::index const& index
) {
    auto& smgr = *global::storage_manager();
    auto sk = smgr.get_storage_key(index_name);
    if(sk.has_value()) {
        if(auto err = recovery::set_storage_option_delete_reserved(index, sk.value())) {
            VLOG_LP(log_error) << "failed to update metadata for delete reservation of index: " << index_name;
            set_error_info(context, err);
            return false;
        }
        return true;
    }
    VLOG_LP(log_warning) << "failed to get storage key for index: " << index_name;
    return false;
}

bool lock_storage_entry(
    request_context& context,
    storage::storage_entry tid,
    std::string_view table_name
) {
    auto& smgr = *global::storage_manager();
    auto& tx = *context.transaction();
    if(! tx.storage_lock()) {
        tx.storage_lock(smgr.create_unique_lock());
    }
    storage::storage_list stg{tid};
    if(! smgr.add_locked_storages(stg, *tx.storage_lock())) {
        // should not happen normally since this is a newly created entry
        auto msg = string_builder{} << "DDL operation was blocked by other DML operation. table:\""
                                    << table_name << "\"" << string_builder::to_string;
        set_error_context(
            context,
            error_code::sql_execution_exception,
            msg,
            status::err_illegal_operation
        );
        utils::print_error(context, msg);
        return false;
    }
    return true;
}

bool create_primary_storage(
    request_context& context,
    yugawara::storage::index const& primary_idx,
    auth::authorized_users_action_set const* authorized_actions,
    auth::action_set const* public_actions,
    storage::storage_entry& tid,
    std::string* serialized
) {
    auto& smgr = *global::storage_manager();
    auto entry_id = storage::index_id_src++;
    auto storage_key = utils::to_big_endian(smgr.generate_surrogate_id());
    auto opt = global::config_pool()->enable_storage_key() ? std::optional<std::string_view>{storage_key} : std::nullopt;

    if(! smgr.add_entry(entry_id, primary_idx.simple_name(), opt, true)) {
        set_error_context(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Table id:" << entry_id << " already exists" << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }

    auto new_se = smgr.find_entry(entry_id);
    if(! new_se) {
        set_error_context(
            context,
            error_code::sql_execution_exception,
            string_builder{} << "Table id:" << entry_id << " not found" << string_builder::to_string,
            status::err_unknown
        );
        return false;
    }
    if(authorized_actions != nullptr) {
        new_se->authorized_actions() = *authorized_actions;
    }
    if(public_actions != nullptr) {
        new_se->public_actions() = *public_actions;
    }

    std::string storage{};
    if(auto err = recovery::create_storage_option(
           primary_idx,
           storage,
           utils::metadata_serializer_option{
               false,
               std::addressof(new_se->authorized_actions()),
               std::addressof(new_se->public_actions()),
               opt
           }
       )) {
        set_error_info(context, err);
        return false;
    }

    if(serialized != nullptr) {
        *serialized = storage;
    }

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage(
           (opt.has_value() ? opt.value() : primary_idx.simple_name()), options); ! stg) {
        set_error_context(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Storage \"" << primary_idx.simple_name() << "\" already exists "
                             << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }
    tid = entry_id;
    return true;
}

bool create_secondary_storage(
    request_context& context,
    yugawara::storage::index const& sec_idx,
    storage::storage_entry primary_entry,
    storage::storage_entry& tid,
    std::string* serialized
) {
    auto& smgr = *global::storage_manager();
    auto entry_id = storage::index_id_src++;
    auto storage_key = utils::to_big_endian(smgr.generate_surrogate_id());
    auto opt = global::config_pool()->enable_storage_key() ? std::optional<std::string_view>{storage_key} : std::nullopt;

    if(! smgr.add_entry(entry_id, sec_idx.simple_name(), opt, false, primary_entry)) {
        set_error_context(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Index id:" << entry_id << " already exists" << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }

    std::string storage{};
    if(auto err = recovery::create_storage_option(
           sec_idx,
           storage,
           utils::metadata_serializer_option{false, nullptr, nullptr, opt}
       )) {
        set_error_info(context, err);
        return false;
    }

    if(serialized != nullptr) {
        *serialized = storage;
    }

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage(
           (opt.has_value() ? opt.value() : sec_idx.simple_name()), options); ! stg) {
        VLOG_LP(log_warning) << "storage " << sec_idx.simple_name() << " already exists ";
        set_error_context(
            context,
            error_code::sql_execution_exception,
            string_builder{} << "Unexpected error." << string_builder::to_string,
            status::err_unknown
        );
        return false;
    }
    tid = entry_id;
    return true;
}

bool acquire_table_lock(
    request_context& context,
    std::string_view table_name,
    storage::storage_entry& out
) {
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name(table_name);
    if(! e.has_value()) {
        set_error_context(
            context,
            error_code::target_not_found_exception,
            string_builder{} << "Table \"" << table_name << "\" not found." << string_builder::to_string,
            status::err_not_found
        );
        return false;
    }
    out = e.value();
    return lock_storage_entry(context, e.value(), table_name);
}

} // namespace jogasaki::executor::common
