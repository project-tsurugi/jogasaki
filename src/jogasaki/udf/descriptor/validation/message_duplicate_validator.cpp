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

#include "message_duplicate_validator.h"

#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/descriptor.pb.h>

#include <jogasaki/logging_helper.h>
#include <jogasaki/udf/log/logging_prefix.h>

#include <jogasaki/udf/descriptor/descriptor_reader.h>

namespace jogasaki::udf::descriptor::validation {

namespace {

struct message_info {
    std::set<std::string> proto_file_names{};
};

[[nodiscard]] std::string build_full_message_name(
    std::string const& pkg, std::string const& parent_name, std::string const& message_name) {
    std::string full_message_name;
    full_message_name.reserve(pkg.size() + parent_name.size() + message_name.size() + 3);

    full_message_name += ".";
    if (!pkg.empty()) {
        full_message_name += pkg;
        full_message_name += ".";
    }
    if (!parent_name.empty()) {
        full_message_name += parent_name;
        full_message_name += ".";
    }
    full_message_name += message_name;

    return full_message_name;
}

void collect_message_definitions(google::protobuf::DescriptorProto const& message,
    std::string const& pkg, std::string const& parent_name, std::string const& proto_file_name,
    std::map<std::string, message_info>& seen) {
    auto const full_message_name = build_full_message_name(pkg, parent_name, message.name());
    seen[full_message_name].proto_file_names.insert(proto_file_name);

    std::string nested_parent_name;
    if (parent_name.empty()) {
        nested_parent_name = message.name();
    } else {
        nested_parent_name.reserve(parent_name.size() + message.name().size() + 1);
        nested_parent_name = parent_name;
        nested_parent_name += ".";
        nested_parent_name += message.name();
    }

    for (auto const& nested : message.nested_type()) {
        collect_message_definitions(nested, pkg, nested_parent_name, proto_file_name, seen);
    }
}

void collect_message_definitions_in_file(
    google::protobuf::FileDescriptorSet const& fds, std::map<std::string, message_info>& seen) {
    for (auto const& file : fds.file()) {
        auto const& pkg = file.package();
        auto const& proto_file_name = file.name();

        for (auto const& message : file.message_type()) {
            collect_message_definitions(message, pkg, "", proto_file_name, seen);
        }
    }
}

[[nodiscard]] bool report_message_definition_duplicates(
    std::map<std::string, message_info> const& seen) {
    bool ok = true;

    for (auto const& [name, info] : seen) {
        if (info.proto_file_names.size() <= 1) { continue; }

        ok = false;

        std::ostringstream oss;

        oss << R"({"event":"udf_message_duplicate","message":")" << name << R"(","protos":[)";

        bool first = true;
        for (auto const& proto : info.proto_file_names) {
            if (!first) { oss << ","; }
            first = false;

            oss << "\"" << proto << "\"";
        }

        oss << "]}";

        LOG_LP(WARNING) << jogasaki::udf::log::prefix << oss.str();
    }

    return ok;
}

} // namespace

bool validate_message_definition_duplicates(std::vector<std::filesystem::path> const& desc_files) {
    if (desc_files.empty()) {
        LOG_LP(WARNING) << jogasaki::udf::log::prefix
                        << "no descriptor files found; message duplication check skipped";
        return true;
    }

    std::map<std::string, message_info> seen{};

    for (auto const& desc_path : desc_files) {
        google::protobuf::FileDescriptorSet fds;
        if (!jogasaki::udf::descriptor::read_file_descriptor_set(desc_path, fds)) { return false; }
        collect_message_definitions_in_file(fds, seen);
    }

    return report_message_definition_duplicates(seen);
}

} // namespace jogasaki::udf::descriptor::validation