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

#include <string_view>

#include <jogasaki/utils/line_number_string.h>

/**
 * @brief declare fatal failure and throw exception
 * @details This macro is used to declare fatal failure and throw exception in case of internal error.
 * This requires the call site to have no noexcept specifier.
 */
#define fail_with_exception() jogasaki::utils::fail_with_exception_impl({}, __FILE__, line_number_string)  //NOLINT
#define fail_with_exception_msg(msg) jogasaki::utils::fail_with_exception_impl(msg, __FILE__, line_number_string)  //NOLINT

/**
 * @brief declare fatal failure without throwing exception
 * @details This macro is in-place replacement of fail_with_exception() for the case where exception is not desired.
 * This logs the error message, but control comes back from these functions, so the caller has to continue with the
 * internal error situation, which is usually not expected to be handled. Use this function just for temporary fixes.
 * In the long term, it should be replaced with fail_with_exception by allowing exception to be thrown.
 */
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
