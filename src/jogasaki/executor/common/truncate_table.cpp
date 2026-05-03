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

#include <jogasaki/auth/action_set.h>
#include <jogasaki/auth/authorized_users_action_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/string_manipulation.h>

#include "ddl_common.h"
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
    ret.reserve(t.columns().size());
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
                ret.emplace_back(
                    generated_sequence_info{*seq_name, std::const_pointer_cast<yugawara::storage::sequence>(s)} //TODO avoid using const pointer cast
                );
            }
        }
    }
    return ret;
}

bool reset_generated_sequences(
    request_context& context,
    yugawara::storage::table const& t,
    yugawara::storage::configurable_provider& provider
) {
    auto seqs = collect_generated_sequences(t, provider);
    // remove existing sequence ids first because accessing system table can hit cc errors
    for(auto&& info : seqs) {
        if(! remove_generated_sequence(context, info.name, *info.seq)) {
            return false;
        }
    }
    // assign new sequence ids while keeping the same sequence object referenced by the table column
    for(auto&& info : seqs) {
        if(! create_generated_sequence(context, *info.seq, false)) {
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
        if(! reserve_delete_index_metadata(context, id, *entry)) {
            error = true;
        }
    });
    return ! error;
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
    // We brand-new the seq id on cc engine, but re-use the seq. def. ids, which are part of configurable_provider.
    if(restart) {
        if(! reset_generated_sequences(context, *t, provider)) {
            return false;
        }
    }

    // mark secondary index storages as delete-reserved in metadata
    std::vector<std::shared_ptr<yugawara::storage::index const>> secondaries{};
    if(! reserve_delete_secondary_indices(context, provider, *t, c.simple_name(), secondaries)) {
        return false;
    }

    // mark primary index storage as delete-reserved in metadata
    auto primary_idx = provider.find_index(c.simple_name());
    if(primary_idx) {
        if(! reserve_delete_index_metadata(context, c.simple_name(), *primary_idx)) {
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

    auto& smgr = *global::storage_manager();
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

    // create new primary storage entry and shirakami storage,
    // inheriting authorized/public actions from the old entry
    auth::authorized_users_action_set inherited_auth{};
    auth::action_set inherited_pub{};
    if(auto old_se = smgr.find_entry(old_storage_id)) {
        inherited_auth = old_se->authorized_actions();
        inherited_pub = old_se->public_actions();
    }
    storage::storage_entry new_primary_id{};
    std::string serialized{};
    if(! create_primary_storage(
           context, *primary_idx, std::addressof(inherited_auth), std::addressof(inherited_pub), new_primary_id,
           std::addressof(serialized)
       )) {
        return false;
    }

    // Sync deserialized stoprage option to provider.
    // Truncate basically re-uses the existing definition and only the difference is the storage key.
    if(auto err = recovery::deserialize_storage_option_into_provider(serialized, provider, provider, true)) {
        set_error_info(context, err);
        return false;
    }

    // hold the write lock on the new primary storage entry until the DDL transaction completes
    if(! lock_storage_entry(context, new_primary_id, c.simple_name())) {
        return false;
    }

    // create new secondary index storages
    for(auto&& entry : secondaries) {
        storage::storage_entry secondary_id{};
        if(! create_secondary_storage(context, *entry, new_primary_id, secondary_id)) {
            return false;
        }
    }

    // tx to remember that it has used the existing storage in order to prevent maintenance thread from deleting the storage
    if (context.transaction()) {
        context.transaction()->add_storage_ref(old_storage_id);
        context.transaction()->add_storage_ref(new_primary_id);
    }

    return true;
}

}  // namespace jogasaki::executor::common
