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
#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <sharksfin/StorageOptions.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/maintenance_storage.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::storage {

std::vector<std::string> maintenance_storage() {  //NOLINT(readability-function-cognitive-complexity)
    std::vector<std::pair<std::string, std::string>> deleted{}; // (name, readable_storage_key)
    auto kvs_db = global::db();
    if (! kvs_db) {
        return {};
    }
    auto& smgr = *global::storage_manager();
    auto candidates = smgr.get_delete_reserved_entries();
    for (auto&& [entry, ctrl] : candidates) {
        bool can_delete = false;
        if (ctrl->is_primary()) {
            can_delete = ctrl->ref_transaction_count() == 0;
        } else {
            auto primary_ctrl = smgr.find_entry(ctrl->primary_entry().value_or(0));
            can_delete = (! primary_ctrl) || primary_ctrl->ref_transaction_count() == 0;
        }
        if (! can_delete) {
            continue;
        }
        auto stg = kvs_db->get_storage(ctrl->derived_storage_key());
        if (stg) {
            // Verify that the KVS metadata also has delete_reserved=true.
            // An aborted DROP TABLE/INDEX rolls back the KVS metadata update but
            // does NOT roll back the in-memory reserve_delete_entry() call, so we
            // must not delete a storage whose KVS metadata says delete_reserved=false.
            sharksfin::StorageOptions opt{};
            if (stg->get_options(opt) == status::ok && ! opt.payload().empty()) {
                proto::metadata::storage::IndexDefinition idef{};
                std::uint64_t v{};
                if (auto err = recovery::validate_extract(opt.payload(), idef, v); ! err) {
                    if (! idef.delete_reserved()) {
                        // KVS metadata does not confirm deletion (e.g. the DDL tx aborted).
                        // Skip — the in-memory state will be corrected on next db restart.
                        continue;
                    }
                }
            }
            auto res = stg->delete_storage();
            if (res != status::ok && res != status::not_found) {
                LOG_LP(ERROR) << "maintenance: failed to delete storage: " << ctrl->derived_storage_key();
                continue;
            }
        }
        auto name = std::string{ctrl->original_name()};
        auto raw_key = ctrl->derived_storage_key();
        std::stringstream key_ss{};
        key_ss << utils::binary_printer{raw_key}.show_hyphen(false);
        smgr.remove_entry(entry);
        deleted.emplace_back(std::move(name), key_ss.str());
    }
    if (VLOG_IS_ON(log_info) && ! deleted.empty()) {
        std::stringstream ss{};
        ss << "deleted storages: [";
        bool first = true;
        for (auto const& [name, key] : deleted) {
            if (! first) {
                ss << ", ";
            }
            ss << "\"" << name << "\"(" << key << ")";
            first = false;
        }
        ss << "]";
        // storages are deleted by the DROP requests, so log by VLOG instead of LOG
        VLOG_LP(log_info) << ss.str();
    }
    std::vector<std::string> result{};
    result.reserve(deleted.size());
    for (auto&& [name, key] : deleted) {
        result.emplace_back(std::move(name));
    }
    return result;
}

} // namespace jogasaki::storage
