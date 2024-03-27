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

#include <string_view>

#include <jogasaki/utils/line_number_string.h>

#define fail_with_exception() jogasaki::utils::fail_with_exception_impl({}, __FILE__, line_number_string)  //NOLINT
#define fail_with_exception_msg(msg) jogasaki::utils::fail_with_exception_impl(msg, __FILE__, line_number_string)  //NOLINT

#define fail_no_exception() jogasaki::utils::fail_no_exception_impl({}, __FILE__, line_number_string)  //NOLINT
#define fail_no_exception_msg(msg) jogasaki::utils::fail_no_exception_impl(msg, __FILE__, line_number_string)  //NOLINT

namespace jogasaki::utils {

[[noreturn]] void fail_with_exception_impl(
    std::string_view msg,
    std::string_view filepath,
    std::string_view position
);

void fail_no_exception_impl(
    std::string_view msg,
    std::string_view filepath,
    std::string_view position
);

}  // namespace jogasaki::utils
