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
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/descriptor.pb.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/udf/descriptor/descriptor_reader.h>
#include <jogasaki/udf/log/logging_prefix.h>

namespace jogasaki::udf::descriptor::validation {

namespace {

struct rpc_info {
    std::string proto_file_name;
    std::string service_name;
    std::string full_rpc_path;
};

[[nodiscard]] std::string build_full_rpc_path(
    std::string const& pkg, std::string const& service_name, std::string const& method_name) {
    std::string full_rpc_path;
    full_rpc_path.reserve(pkg.size() + service_name.size() + method_name.size() + 3);

    full_rpc_path += "/";
    if (!pkg.empty()) {
        full_rpc_path += pkg;
        full_rpc_path += ".";
    }
    full_rpc_path += service_name;
    full_rpc_path += "/";
    full_rpc_path += method_name;

    return full_rpc_path;
}

[[nodiscard]] bool insert_rpc_info(std::map<std::string, rpc_info>& seen,
    std::string const& method_name, rpc_info const& current) {
    auto const result = seen.emplace(method_name, current);
    auto const& it = result.first;
    auto const inserted = result.second;

    if (inserted) { return true; }

    LOG_LP(WARNING) << jogasaki::udf::log::prefix << "RPC name duplicated: " << method_name
                    << " first: proto=" << it->second.proto_file_name
                    << " service=" << it->second.service_name << " rpc=" << it->second.full_rpc_path
                    << " second: proto=" << current.proto_file_name
                    << " service=" << current.service_name << " rpc=" << current.full_rpc_path;
    return false;
}

[[nodiscard]] bool validate_rpc_methods_in_file(
    google::protobuf::FileDescriptorSet const& fds, std::map<std::string, rpc_info>& seen) {
    for (auto const& file : fds.file()) {
        auto const& pkg = file.package();
        auto const& proto = file.name();

        for (auto const& service : file.service()) {
            auto const& service_name = service.name();

            for (auto const& method : service.method()) {
                auto const& method_name = method.name();

                rpc_info current;
                current.proto_file_name = proto;
                current.service_name = service_name;
                current.full_rpc_path = build_full_rpc_path(pkg, service_name, method_name);

                if (!insert_rpc_info(seen, method_name, current)) { return false; }
            }
        }
    }
    return true;
}

} // namespace

bool validate_rpc_method_duplicates(std::vector<std::filesystem::path> const& desc_files) {
    if (desc_files.empty()) {
        LOG_LP(WARNING) << jogasaki::udf::log::prefix
                        << "no descriptor files found; RPC duplication check skipped";
        return true;
    }

    std::map<std::string, rpc_info> seen{};

    for (auto const& desc_path : desc_files) {
        google::protobuf::FileDescriptorSet fds;
        if (!jogasaki::udf::descriptor::read_file_descriptor_set(desc_path, fds)) { return false; }

        if (!validate_rpc_methods_in_file(fds, seen)) { return false; }
    }

    return true;
}

} // namespace jogasaki::udf::descriptor::validation
