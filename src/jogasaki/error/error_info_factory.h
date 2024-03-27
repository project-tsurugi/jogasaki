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

#include <iosfwd>
#include <memory>
#include <string_view>
#include <glog/logging.h>

#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/line_number_string.h>

namespace jogasaki::error {

#define create_error_info(code, msg, st) jogasaki::error::create_error_info_impl((code), (msg), __FILE__, line_number_string, (st), false) //NOLINT
#define set_error(rctx, code, msg, st) jogasaki::error::set_error_impl((rctx), (code), (msg), __FILE__, line_number_string, (st), false) //NOLINT
#define create_error_from_exception(e) jogasaki::error::create_error_from_exception_impl((e), __FILE__, line_number_string) //NOLINT

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
 * @brief create and set error info to the request context and transaction context
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

/**
 * @brief set given error info to the request context and transaction context
 * @param rctx request context to set error
 * @param info error info to be set (if null, the function does nothing)
 */
void set_error_info(
    request_context& rctx,
    std::shared_ptr<error_info> const& info
);

/**
 * @brief create error info from the given exception
 * @param e the exception to extract error info from
 * The exception must be derived from std::exception and have get_code() and get_status() methods.
 * If the exception is thrown by takatori::util::throw_exception, the stack trace is also included in the error info.
 */
template <class T>
std::shared_ptr<error_info> create_error_from_exception_impl(
    T const& e,
    std::string_view filepath,
    std::string_view position
) {
    std::stringstream ss{};
    if(auto trace = takatori::util::find_trace(e)) {
        ss << *trace;
    }
    auto info = std::make_shared<error_info>(
        e.get_code(),
        e.what(),
        filepath,
        position,
        ss.str()
    );
    info->status(e.get_status());
    VLOG_LP(log_trace) << "error_info:" << *info;
    return info;
}

}  // namespace jogasaki::error
