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

#include <google/protobuf/descriptor.pb.h>
#include <jogasaki/udf/descriptor/descriptor_reader.h>

namespace jogasaki::udf::descriptor::validation {

namespace {

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

[[nodiscard]] std::string normalize_type_name(std::string const& name) {
    if (name.empty()) { return name; }
    if (name.front() == '.') { return name; }

    std::string result;
    result.reserve(name.size() + 1);
    result += ".";
    result += name;
    return result;
}

void collect_message_definitions(google::protobuf::DescriptorProto const& message,
    std::string const& pkg, std::string const& parent_name, std::string const& proto_file_name,
    message_diagnostics& diagnostics) {
    auto const full_message_name = build_full_message_name(pkg, parent_name, message.name());
    diagnostics[full_message_name].defining_protos.insert(proto_file_name);

    std::string nested_parent_name;
    if (parent_name.empty()) {
        nested_parent_name = message.name();
    } else {
        nested_parent_name.reserve(parent_name.size() + message.name().size() + 1);
        nested_parent_name = parent_name;
        nested_parent_name += ".";
        nested_parent_name += message.name();
    }

    for (auto const& field : message.field()) {
        if (field.type() == google::protobuf::FieldDescriptorProto::TYPE_MESSAGE &&
            !field.type_name().empty()) {
            diagnostics[normalize_type_name(field.type_name())].referring_protos.insert(
                proto_file_name);
        }
    }

    for (auto const& nested : message.nested_type()) {
        collect_message_definitions(nested, pkg, nested_parent_name, proto_file_name, diagnostics);
    }
}

void collect_message_references_in_services(
    google::protobuf::FileDescriptorProto const& file, message_diagnostics& diagnostics) {
    auto const& proto_file_name = file.name();

    for (auto const& service : file.service()) {
        for (auto const& method : service.method()) {
            if (!method.input_type().empty()) {
                diagnostics[normalize_type_name(method.input_type())].referring_protos.insert(
                    proto_file_name);
            }
            if (!method.output_type().empty()) {
                diagnostics[normalize_type_name(method.output_type())].referring_protos.insert(
                    proto_file_name);
            }
        }
    }
}

void collect_message_information_in_file(
    google::protobuf::FileDescriptorProto const& file, message_diagnostics& diagnostics) {
    auto const& pkg = file.package();
    auto const& proto_file_name = file.name();

    for (auto const& message : file.message_type()) {
        collect_message_definitions(message, pkg, "", proto_file_name, diagnostics);
    }

    collect_message_references_in_services(file, diagnostics);
}

[[nodiscard]] message_diagnostics filter_duplicates(message_diagnostics const& all) {
    message_diagnostics duplicates{};
    for (auto const& [message_name, diag] : all) {
        if (diag.defining_protos.size() > 1) { duplicates.emplace(message_name, diag); }
    }
    return duplicates;
}

} // namespace

message_diagnostics find_message_definition_duplicates(
    std::vector<std::filesystem::path> const& desc_files) {
    if (desc_files.empty()) { return {}; }

    message_diagnostics all{};

    for (auto const& desc_path : desc_files) {
        google::protobuf::FileDescriptorSet fds;
        if (!jogasaki::udf::descriptor::read_file_descriptor_set(desc_path, fds)) { return {}; }

        for (auto const& file : fds.file()) {
            collect_message_information_in_file(file, all);
        }
    }

    return filter_duplicates(all);
}

} // namespace jogasaki::udf::descriptor::validation