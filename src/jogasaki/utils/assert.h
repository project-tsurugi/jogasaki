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
#define assert_with_exception(cond, ...) ::jogasaki::utils::assert_with_exception_impl(#cond, (cond), {stringify_va_args(__VA_ARGS__)}, __VA_ARGS__)

#define utils_details_stringify9(x,...) #x, utils_details_stringify8(__VA_ARGS__)
#define utils_details_stringify8(x,...) #x, utils_details_stringify7(__VA_ARGS__)
#define utils_details_stringify7(x,...) #x, utils_details_stringify6(__VA_ARGS__)
#define utils_details_stringify6(x,...) #x, utils_details_stringify5(__VA_ARGS__)
#define utils_details_stringify5(x,...) #x, utils_details_stringify4(__VA_ARGS__)
#define utils_details_stringify4(x,...) #x, utils_details_stringify3(__VA_ARGS__)
#define utils_details_stringify3(x,...) #x, utils_details_stringify2(__VA_ARGS__)
#define utils_details_stringify2(x,...) #x, utils_details_stringify1(__VA_ARGS__)
#define utils_details_stringify1(x,...) #x

#define utils_details_stringify_impl2(count, ...) utils_details_stringify ## count (__VA_ARGS__)
#define utils_details_stringify_impl(count, ...) utils_details_stringify_impl2(count, __VA_ARGS__)

#define utils_details_va_nargs_impl(_1, _2, _3, _4, _5, _6, _7, _8, _9, N, ...) N
#define utils_details_va_nargs(...) utils_details_va_nargs_impl(__VA_ARGS__, 9, 8, 7, 6, 5, 4, 3, 2, 1)
#define stringify_va_args(...) utils_details_stringify_impl(utils_details_va_nargs(__VA_ARGS__), __VA_ARGS__)
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
    builder << names[N] << ":" << head << " ";
    add_name_value_to_string<N+1>(builder, names, std::forward<Tails>(args)...);
}

template <class ... Args>
void add_var_name_value(string_builder& builder, std::vector<char const*> const& names, Args&& ...args) {
    add_name_value_to_string<0>(builder, names, args...);
}

}  // namespace details

template <class ...Args>
void assert_with_exception_impl(char const* str, bool cond, std::initializer_list<char const*> names, Args&& ...args) {
    if(! cond) {
        std::vector<char const*> n{names};
        auto builder = string_builder{}
            << base_filename()
            << " condition \'" << str << "\' failed ";
        details::add_var_name_value(builder, names, args...);
        throw_exception(std::logic_error{builder << string_builder::to_string});

    }
}

}  // namespace jogasaki::utils
