/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <functional>

#include <takatori/scalar/expression.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/match.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/coalesce.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/conditional.h>
#include <takatori/scalar/extension.h>
#include <takatori/scalar/function_call.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/let.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/variable_reference.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/data/any.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::executor::process::impl::expression::details {

inline jogasaki::data::any return_unsupported() {
    return {std::in_place_type<error>, error(error_kind::unsupported)};
}

inline std::string_view trim_spaces(std::string_view src) {
    auto b = std::find_if(src.begin(), src.end(), [](char c){
        return c != ' ';
    });
    auto e = std::find_if(src.rbegin(), src.rend(), [](char c){
        return c != ' ';
    });
    return {b, static_cast<std::size_t>(std::distance(b, e.base()))};
}

inline bool is_prefix_of_case_insensitive(std::string_view a, std::string_view b) {
    return ! a.empty() && a.size() <= b.size() &&
        std::equal(a.begin(), a.end(), b.begin(), [](auto l, auto r) {
            return std::tolower(l) == std::tolower(r);
        });
}

} // namespace
