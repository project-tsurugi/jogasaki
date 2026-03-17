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

#include <vector>

#include <jogasaki/udf/descriptor/descriptor_analyzer.h>
#include <jogasaki/udf/enum_types.h>

namespace jogasaki::udf::descriptor::validation {

[[nodiscard]] plugin::udf::rpc_duplicate_check_status validate_rpc_method_duplicates(
    std::vector<rpc_method_entry> const& rpc_methods);

} // namespace jogasaki::udf::descriptor::validation
