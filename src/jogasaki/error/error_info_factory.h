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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/error_code.h>
#include <jogasaki/request_context.h>
#include <jogasaki/error/error_info.h>

namespace jogasaki::error {

#define stringify1(x) #x //NOLINT
#define stringify2(x) stringify1(x) //NOLINT
#define line_number_string stringify2(__LINE__)

#define create_error_info(code, msg, st) jogasaki::error::create_error_info_impl(code, msg, __FILE__, line_number_string, st, false) //NOLINT
#define set_error(rctx, code, msg, st) jogasaki::error::set_error_impl(rctx, code, msg, __FILE__, line_number_string, st, false) //NOLINT

std::shared_ptr<error_info> create_error_info_impl(
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st,
    bool append_stacktrace
);

std::shared_ptr<error_info> create_error_info_with_stack_impl(
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st,
    std::string_view stacktrace
);

/**
 * @brief set error info to the request context and transaction context
 * @param rctx request context to set error
 * @param info error info to be set
 */
void set_error_impl(
    request_context& rctx,
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st,
    bool append_stacktrace
);

}
