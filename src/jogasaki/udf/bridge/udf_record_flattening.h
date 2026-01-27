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
#include <cstddef>
#include <string_view>
#include <vector>

#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/udf/plugin_api.h>
#include <jogasaki/udf/data/udf_semantic_type.h>
namespace jogasaki::udf::bridge {
[[nodiscard]] bool is_special_nested_record(std::string_view rn);
[[nodiscard]] jogasaki::executor::function::table_valued_function_info::columns_type
build_tvf_columns(plugin::udf::function_descriptor const& fn);
[[nodiscard]] std::size_t count_effective_columns(plugin::udf::record_descriptor const& rec);
[[nodiscard]] std::vector<jogasaki::udf::data::udf_wire_kind>
build_output_wire_kinds(plugin::udf::function_descriptor const& fn) ;
} // namespace jogasaki::udf::bridge
