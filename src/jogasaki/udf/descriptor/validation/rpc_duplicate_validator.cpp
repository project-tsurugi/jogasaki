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
#include "rpc_duplicate_validator.h"

#include <map>
#include <string>

#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/udf/enum_types.h>
#include <jogasaki/udf/log/logging_prefix.h>

namespace jogasaki::udf::descriptor::validation {

namespace {

struct rpc_info {
    std::string proto_file_name;
    std::string service_name;
    std::string full_rpc_path;
};

[[nodiscard]] bool insert_rpc_info(
    std::map<std::string, rpc_info>& seen, rpc_method_entry const& entry) {
    rpc_info current;
    current.proto_file_name = entry.proto_file_name;
    current.service_name = entry.service_name;
    current.full_rpc_path = entry.full_rpc_path;

    auto const result = seen.emplace(entry.method_name, current);
    auto const& it = result.first;
    auto const inserted = result.second;

    if (inserted) { return true; }

    LOG_LP(WARNING) << jogasaki::udf::log::prefix << "RPC name duplicated: " << entry.method_name
                    << " first: proto=" << it->second.proto_file_name
                    << " service=" << it->second.service_name << " rpc=" << it->second.full_rpc_path
                    << " second: proto=" << current.proto_file_name
                    << " service=" << current.service_name << " rpc=" << current.full_rpc_path;
    return false;
}

} // namespace

plugin::udf::rpc_duplicate_check_status validate_rpc_method_duplicates(
    std::vector<rpc_method_entry> const& rpc_methods) {
    std::map<std::string, rpc_info> seen{};

    for (auto const& entry : rpc_methods) {
        if (!insert_rpc_info(seen, entry)) {
            return plugin::udf::rpc_duplicate_check_status::rpc_name_duplicated;
        }
    }

    return plugin::udf::rpc_duplicate_check_status::ok;
}

} // namespace jogasaki::udf::descriptor::validation