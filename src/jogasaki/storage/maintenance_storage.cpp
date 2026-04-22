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
    auto& smgr = *global::storage_manager();
    auto candidates = smgr.get_delete_reserved_entries();
    deleted.reserve(candidates.size());
    for (auto&& entry : candidates) {
        auto ctrl = smgr.find_entry(entry);
        if (! ctrl) {
            // should not happen normally
            continue;
        }
        bool can_delete = false;
        if (ctrl->is_primary()) {
            can_delete = ctrl->ref_transaction_count() == 0;
        } else {
            // secondary index
            if (ctrl->primary_entry().has_value()) {
                auto primary_ctrl = smgr.find_entry(ctrl->primary_entry().value());
                can_delete = (! primary_ctrl) || primary_ctrl->ref_transaction_count() == 0;
            } else {
                // secondary index without primary entry
                // (e.g. because primary is also delete reserved and recovery has been done)
                // can be deleted independently because there is no need to track ref count after recovery
                can_delete = true;
            }
        }
        if (! can_delete) {
            continue;
        }
        auto stg = global::db()->get_storage(ctrl->derived_storage_key());
        if (stg) {
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
