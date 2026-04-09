/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <initializer_list>
#include <stdexcept>
#include <utility>
#include <vector>

#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/utils/base_filename.h>

//  NOLINTBEGIN(cppcoreguidelines-macro-usage)

// ---------------------------------------------------------------------------
// Internal helpers (utils_details_ prefix — not part of the public API)
// ---------------------------------------------------------------------------

// Stringify 0..9 arguments as a comma-separated list of string literals.
//   utils_details_str0()        ->  (empty)
//   utils_details_str1(a)       ->  "a"
//   utils_details_str2(a, b)    ->  "a", "b"
//   ...
#define utils_details_str0(...)
#define utils_details_str1(a,...)  #a
#define utils_details_str2(a,...)  #a, utils_details_str1(__VA_ARGS__)
#define utils_details_str3(a,...)  #a, utils_details_str2(__VA_ARGS__)
#define utils_details_str4(a,...)  #a, utils_details_str3(__VA_ARGS__)
#define utils_details_str5(a,...)  #a, utils_details_str4(__VA_ARGS__)
#define utils_details_str6(a,...)  #a, utils_details_str5(__VA_ARGS__)
#define utils_details_str7(a,...)  #a, utils_details_str6(__VA_ARGS__)
#define utils_details_str8(a,...)  #a, utils_details_str7(__VA_ARGS__)
#define utils_details_str9(a,...)  #a, utils_details_str8(__VA_ARGS__)

// Count 0..9 arguments. Uses the GNU ##__VA_ARGS__ extension to handle the
// zero-argument case (GCC/Clang, C++17 compatible).
// TODO: Replace ##__VA_ARGS__ with __VA_OPT__ when C++20 is adopted by this project.
#define utils_details_count_impl(_0,_1,_2,_3,_4,_5,_6,_7,_8,_9,N,...) N
#define utils_details_count(...) \
    utils_details_count_impl(x, ##__VA_ARGS__, 9,8,7,6,5,4,3,2,1,0)

// Select the right strN helper for the given count.
// Two levels of indirection are required: the outer level (stringify2) receives N as a
// plain parameter so the preprocessor expands it to a literal; the inner level (str_pick)
// then token-pastes the already-expanded literal with "str".  A single level would skip
// expansion because ## suppresses it for adjacent parameters.
#define utils_details_str_pick(N,...) utils_details_str ## N(__VA_ARGS__)
#define utils_details_stringify2(N,...) utils_details_str_pick(N, ##__VA_ARGS__)
#define utils_details_stringify(...) \
    utils_details_stringify2(utils_details_count(__VA_ARGS__), ##__VA_ARGS__)

// ---------------------------------------------------------------------------
// Public macro
// ---------------------------------------------------------------------------

/**
 * @brief Assert a condition; throw std::logic_error on failure.
 * @details Usage:
 *   @code
 *   assert_with_exception(cond);            // assert condition only
 *   assert_with_exception(cond, a, b);      // assert and print variables on failure
 *   @endcode
 *
 * On failure the exception message includes the source file name, the stringified
 * condition, and for each extra argument a "name:value" pair.
 *
 */
#define assert_with_exception(cond, ...) \
    ::jogasaki::utils::assert_with_exception_impl(                        \
        #cond,                                                             \
        static_cast<bool>(cond),                                          \
        {utils_details_stringify(__VA_ARGS__)},                       \
        ##__VA_ARGS__)

//  NOLINTEND(cppcoreguidelines-macro-usage)

namespace jogasaki::utils {

using takatori::util::string_builder;
using takatori::util::throw_exception;

namespace details {

template <int N>
void add_name_value_to_string(string_builder& builder, std::vector<char const*> const& names) {
    (void) builder;
    (void) names;
}

template<int N, class Head, class... Tails>
void add_name_value_to_string(
    string_builder& builder,
    std::vector<char const*> const& names,
    Head&& head,
    Tails&&... args
) {
    builder << names[N] << ":" << std::forward<Head>(head) << " ";
    add_name_value_to_string<N+1>(builder, names, std::forward<Tails>(args)...);
}

}  // namespace details

/**
 * @brief Implementation detail for assert_with_exception; do not call directly.
 */
template<class ...Args>
void assert_with_exception_impl(
    char const* str,
    bool cond,
    std::initializer_list<char const*> names,
    Args&&... args
) {
    if(! cond) {
        auto builder = string_builder{}
            << base_filename()
            << " condition \'" << str << "\' failed";
        if(names.size() > 0) {
            builder << " ";
            std::vector<char const*> names_vec{names};
            details::add_name_value_to_string<0>(builder, names_vec, std::forward<Args>(args)...);
        }
        throw_exception(std::logic_error{builder << string_builder::to_string});
    }
}

}  // namespace jogasaki::utils
