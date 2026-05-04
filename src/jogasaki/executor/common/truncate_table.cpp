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
#include "truncate_table.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <takatori/statement/truncate_table_option_kind.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/column_value_kind.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
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
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/string_manipulation.h>
#include <jogasaki/utils/surrogate_id_utils.h>

#include "acquire_table_lock.h"
#include "validate_alter_table_auth.h"

namespace jogasaki::executor::common {

using takatori::util::string_builder;

truncate_table::truncate_table(takatori::statement::truncate_table& ct) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind truncate_table::kind() const noexcept {
    return model::statement_kind::truncate_table;
}

namespace {

// information about an auto-generated sequence (for implicit PK or generated identity columns)
struct generated_sequence_info {
    std::string name{};
    std::shared_ptr<yugawara::storage::sequence> seq{};
};

std::vector<generated_sequence_info> collect_generated_sequences(
    yugawara::storage::table const& t,
    yugawara::storage::configurable_provider& provider
) {
    std::vector<generated_sequence_info> ret{};
    for(auto&& col : t.columns()) {
        std::optional<std::string> seq_name{};
        if(utils::is_prefix(col.simple_name(), generated_pkey_column_prefix)) {
            seq_name = std::string{col.simple_name()};
        } else if(col.default_value().kind() == yugawara::storage::column_value_kind::sequence) {
            auto& dv = col.default_value().element<yugawara::storage::column_value_kind::sequence>();
            if(utils::is_prefix(dv->simple_name(), generated_sequence_name_prefix)) {
                seq_name = std::string{dv->simple_name()};
            }
        }
        if(seq_name) {
            if(auto s = provider.find_sequence(*seq_name)) {
                ret.push_back({*seq_name, std::const_pointer_cast<yugawara::storage::sequence>(s)});
            }
        }
    }
    return ret;
}

bool remove_existing_sequence_id(
    request_context& context,
    yugawara::storage::sequence const& seq
) {
    if(! seq.definition_id().has_value()) {
        return true;
    }
    auto def_id = seq.definition_id().value();
    try {
        if(! context.sequence_manager()->remove_sequence(def_id, context.transaction()->object().get())) {
            // continue clean-up as much as possible
            VLOG_LP(log_info) << "sequence '" << seq.simple_name() << "' not found";
        }
    } catch(executor::sequence::exception const& e) {
        VLOG_LP(log_error) << "removing sequence '" << seq.simple_name() << "' failed";
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

bool assign_new_sequence_id(
    request_context& context,
    yugawara::storage::sequence& seq
) {
    executor::sequence::metadata_store ms{*context.transaction()->object()};
    std::size_t def_id{};
    try {
        ms.find_next_empty_def_id(def_id);
        seq.definition_id(def_id);
        context.sequence_manager()->register_sequence(
            std::addressof(static_cast<kvs::transaction&>(*context.transaction())),
            *seq.definition_id(),
            seq.simple_name(),
            seq.initial_value(),
            seq.increment_value(),
            seq.min_value(),
            seq.max_value(),
            seq.cycle(),
            true,
            true
        );
    } catch(executor::sequence::exception& e) {
        handle_kvs_errors(context, e.get_status());
        handle_generic_error(context, e.get_status(), error_code::sql_execution_exception);
        return false;
    }
    return true;
}

bool reset_generated_sequences(
    request_context& context,
    yugawara::storage::table const& t,
    yugawara::storage::configurable_provider& provider
) {
    auto seqs = collect_generated_sequences(t, provider);
    // remove existing sequence ids first because accessing system table can hit cc errors
    for(auto&& info : seqs) {
        if(! remove_existing_sequence_id(context, *info.seq)) {
            return false;
        }
    }
    // assign new sequence ids while keeping the same sequence object referenced by the table column
    for(auto&& info : seqs) {
        if(! assign_new_sequence_id(context, *info.seq)) {
            return false;
        }
    }
    return true;
}

bool reserve_delete_secondary_indices(
    request_context& context,
    yugawara::storage::configurable_provider& provider,
    yugawara::storage::table const& t,
    std::string_view table_name,
    std::vector<std::shared_ptr<yugawara::storage::index const>>& out_secondaries
) {
    auto& smgr = *global::storage_manager();
    bool error = false;
    provider.each_table_index(t, [&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const& entry) {
        if(error) {
            return;
        }
        if(table_name == entry->simple_name()) {
            // skip primary
            return;
        }
        out_secondaries.emplace_back(entry);
        auto sk = smgr.get_storage_key(id);
        if(sk.has_value()) {
            if(auto err = recovery::set_storage_option_delete_reserved(*entry, sk.value())) {
                VLOG_LP(log_error) << "failed to update metadata for delete reservation of secondary index: " << id;
                set_error_info(context, err);
                error = true;
                return;
            }
        } else {
            VLOG_LP(log_warning) << "failed to get storage key for index: " << id;
            error = true;
            return;
        }
    });
    return ! error;
}

bool create_new_primary_storage(
    request_context& context,
    yugawara::storage::index const& primary_idx,
    storage::storage_entry old_storage_id,
    storage::storage_entry& out_new_storage_id
) {
    auto& smgr = *global::storage_manager();
    auto tid = storage::index_id_src++;
    auto storage_key = utils::to_big_endian(smgr.generate_surrogate_id());
    auto opt = global::config_pool()->enable_storage_key() ? std::optional<std::string_view>{storage_key} : std::nullopt;

    if(! smgr.add_entry(tid, primary_idx.simple_name(), opt, true)) {
        set_error_context(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Table id:" << tid << " already exists" << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }

    auto new_se = smgr.find_entry(tid);
    if(! new_se) {
        set_error_context(
            context,
            error_code::sql_execution_exception,
            string_builder{} << "Table id:" << tid << " not found" << string_builder::to_string,
            status::err_unknown
        );
        return false;
    }
    // inherit authorized actions and public actions from the old storage entry
    if(auto old_se = smgr.find_entry(old_storage_id)) {
        new_se->authorized_actions() = old_se->authorized_actions();
        new_se->public_actions() = old_se->public_actions();
    }

    std::string storage{};
    if(auto err = recovery::create_storage_option(
           primary_idx,
           storage,
           utils::metadata_serializer_option{
               false,
               std::addressof(new_se->authorized_actions()),
               nullptr,
               opt
           }
       )) {
        set_error_info(context, err);
        return false;
    }

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage((opt.has_value() ? opt.value() : primary_idx.simple_name()), options); ! stg) {
        set_error_context(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Storage \"" << primary_idx.simple_name() << "\" already exists " << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }
    out_new_storage_id = tid;
    return true;
}

bool create_new_secondary_storage(
    request_context& context,
    yugawara::storage::index const& sec_idx,
    storage::storage_entry primary_entry
) {
    auto& smgr = *global::storage_manager();
    auto tid = storage::index_id_src++;
    auto storage_key = utils::to_big_endian(smgr.generate_surrogate_id());
    auto opt = global::config_pool()->enable_storage_key() ? std::optional<std::string_view>{storage_key} : std::nullopt;

    if(! smgr.add_entry(tid, sec_idx.simple_name(), opt, false, primary_entry)) {
        set_error_context(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Index id:" << tid << " already exists" << string_builder::to_string,
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

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage((opt.has_value() ? opt.value() : sec_idx.simple_name()), options); ! stg) {
        VLOG_LP(log_warning) << "storage " << sec_idx.simple_name() << " already exists ";
        set_error_context(
            context,
            error_code::sql_execution_exception,
            string_builder{} << "Unexpected error." << string_builder::to_string,
            status::err_unknown
        );
        return false;
    }
    return true;
}

}  // namespace

bool truncate_table::operator()(request_context& context) const {  //NOLINT(readability-function-cognitive-complexity)
    assert_with_exception(context.storage_provider());
    auto& provider = *context.storage_provider();
    auto& c = yugawara::binding::extract<yugawara::storage::table>(ct_->target());
    auto t = provider.find_table(c.simple_name());
    if(t == nullptr) {
        set_error_context(
            context,
            error_code::target_not_found_exception,
            string_builder{} << "Table \"" << c.simple_name() << "\" not found." << string_builder::to_string,
            status::err_not_found
        );
        return false;
    }

    storage::storage_entry old_storage_id{};
    if(! acquire_table_lock(context, c.simple_name(), old_storage_id)) {
        return false;
    }
    if(! validate_alter_table_auth(context, old_storage_id)) {
        return false;
    }

    bool restart =
        ct_->options().contains(takatori::statement::truncate_table_option_kind::restart_identity);

    // For RESTART IDENTITY, replace existing sequence ids with new ones first
    // (accessing the system table can cause cc errors, so do it early to bail out cleanly).
    if(restart) {
        if(! reset_generated_sequences(context, *t, provider)) {
            return false;
        }
    }

    auto& smgr = *global::storage_manager();

    // mark secondary index storages as delete-reserved in metadata
    std::vector<std::shared_ptr<yugawara::storage::index const>> secondaries{};
    if(! reserve_delete_secondary_indices(context, provider, *t, c.simple_name(), secondaries)) {
        return false;
    }

    // mark primary index storage as delete-reserved in metadata
    auto primary_idx = provider.find_index(c.simple_name());
    if(primary_idx) {
        auto sk = smgr.get_storage_key(c.simple_name());
        if(sk.has_value()) {
            if(auto err = recovery::set_storage_option_delete_reserved(*primary_idx, sk.value())) {
                VLOG_LP(log_warning) << "failed to update metadata for delete reservation of primary index: " << c.simple_name();
                set_error_info(context, err);
                return false;
            }
        } else {
            VLOG_LP(log_warning) << "failed to get storage key for index: " << c.simple_name();
            return false;
        }
    } else {
        // should not happen normally because the table existed in the provider
        set_error_context(
            context,
            error_code::sql_execution_exception,
            string_builder{} << "Primary index for table \"" << c.simple_name() << "\" not found." << string_builder::to_string,
            status::err_not_found
        );
        return false;
    }

    // reserve old secondary index storage entries for lazy deletion
    for(auto&& entry : secondaries) {
        auto e = smgr.find_by_name(entry->simple_name());
        if(! e) {
            VLOG_LP(log_warning) << "failed to find storage entry name:" << entry->simple_name();
            continue;
        }
        smgr.reserve_delete_entry(e.value());
    }
    // reserve old primary storage entry for lazy deletion
    smgr.reserve_delete_entry(old_storage_id);

    // create new primary storage entry and shirakami storage
    storage::storage_entry new_primary_id{};
    if(! create_new_primary_storage(context, *primary_idx, old_storage_id, new_primary_id)) {
        return false;
    }

    // hold the write lock on the new primary storage entry until the DDL transaction completes
    auto& tx = *context.transaction();
    if(! tx.storage_lock()) {
        tx.storage_lock(smgr.create_unique_lock());
    }
    storage::storage_list new_primary_list{new_primary_id};
    if(! smgr.add_locked_storages(new_primary_list, *tx.storage_lock())) {
        // should not happen normally since this is a freshly created entry
        auto msg = string_builder{} << "DDL operation was blocked by other DML operation. table:\"" << c.simple_name()
                                    << "\"" << string_builder::to_string;
        set_error_context(
            context,
            error_code::sql_execution_exception,
            msg,
            status::err_illegal_operation
        );
        return false;
    }

    // create new secondary index storages
    for(auto&& entry : secondaries) {
        if(! create_new_secondary_storage(context, *entry, new_primary_id)) {
            return false;
        }
    }
    return true;
}

}  // namespace jogasaki::executor::common
