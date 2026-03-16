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

#include <map>
#include <set>
#include <string>
#include <filesystem>
#include <vector>

namespace jogasaki::udf::descriptor::validation {

struct message_diagnostic {
    std::set<std::string> defining_protos{};
    std::set<std::string> referring_protos{};
};

using message_diagnostics = std::map<std::string, message_diagnostic>;

[[nodiscard]] message_diagnostics find_message_definition_duplicates(
    std::vector<std::filesystem::path> const& desc_files);

}  // namespace jogasaki::udf::descriptor::validation
