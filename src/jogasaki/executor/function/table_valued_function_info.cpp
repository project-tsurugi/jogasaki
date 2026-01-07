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
#include "table_valued_function_info.h"

#include <utility>

namespace jogasaki::executor::function {

table_valued_function_column::table_valued_function_column(std::string name) :
    name_(std::move(name))
{}

std::string const& table_valued_function_column::name() const noexcept {
    return name_;
}

table_valued_function_info::table_valued_function_info(
    table_valued_function_kind kind,
    table_valued_function_type function_body,
    std::size_t arg_count,
    columns_type columns
) :
    kind_(kind),
    function_body_(std::move(function_body)),
    arg_count_(arg_count),
    columns_(std::move(columns))
{}

table_valued_function_type const& table_valued_function_info::function_body() const noexcept {
    return function_body_;
}

std::size_t table_valued_function_info::arg_count() const noexcept {
    return arg_count_;
}

table_valued_function_info::columns_type const& table_valued_function_info::columns() const noexcept {
    return columns_;
}

}  // namespace jogasaki::executor::function
