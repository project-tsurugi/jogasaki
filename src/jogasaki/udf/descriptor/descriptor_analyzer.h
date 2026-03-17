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
#pragma once

#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <jogasaki/udf/enum_types.h>

namespace jogasaki::udf::descriptor {

struct message_diagnostic {
    std::set<std::string> defining_protos{};
    std::set<std::string> referring_protos{};
};

using message_diagnostics = std::map<std::string, message_diagnostic>;

struct rpc_method_entry {
    std::string proto_file_name{};
    std::string service_name{};
    std::string method_name{};
    std::string full_rpc_path{};
};

struct descriptor_load_error {
    std::filesystem::path path{};
    plugin::udf::descriptor_read_status status{};
};
struct descriptor_analysis_result {
    // intentionally public: simple POD-like result container
    // NOLINTNEXTLINE(misc-non-private-member-variables-in-classes)
    std::vector<descriptor_load_error> errors{};
    // intentionally public: simple POD-like result container
    // NOLINTNEXTLINE(misc-non-private-member-variables-in-classes)
    std::vector<rpc_method_entry> rpc_methods{};
    // intentionally public: simple POD-like result container
    // NOLINTNEXTLINE(misc-non-private-member-variables-in-classes)
    message_diagnostics message_info{};

    [[nodiscard]] bool has_error() const noexcept { return !errors.empty(); }
};
[[nodiscard]] descriptor_analysis_result analyze_descriptors(
    std::vector<std::filesystem::path> const& desc_files);

} // namespace jogasaki::udf::descriptor
