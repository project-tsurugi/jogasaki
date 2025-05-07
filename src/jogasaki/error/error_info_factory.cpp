/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "error_info_factory.h"

#include <sstream>
#include <string>
#include <type_traits>

#include <takatori/util/stacktrace.h>

#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::error {

std::shared_ptr<error_info> create_error_info_with_stack_impl(
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st,
    std::string_view stacktrace
) {
    auto info = std::make_shared<error_info>(code, message, filepath, position, stacktrace);
    info->status(st);
    if(! stacktrace.empty()) {
        // currently assuming the error is severe if stacktrace is provided
        LOG_LP(ERROR) << "unexpected internal error " << *info;
    } else {
        VLOG_LP(log_trace) << "error_info:" << *info;
    }
    return info;
}

std::shared_ptr<error_info> create_error_info_impl(
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st,
    bool append_stacktrace
) {
    std::stringstream ss{};
    if(append_stacktrace) {
        // be careful - stacktrace is expensive esp. on debug build
        ss << ::boost::stacktrace::stacktrace{};
    }
    return create_error_info_with_stack_impl(code, message, filepath, position, st, ss.str());
}

void set_error_info(
    request_context& rctx,
    std::shared_ptr<error_info> const& info
) {
    if(! info) return;
    auto st = info->status();
    auto code = info->code();
    rctx.status_code(st, info->message());
    if(rctx.error_info(info)) {
        if(rctx.transaction()) {
            if(code == error_code::inactive_transaction_exception || st == status::err_inactive_transaction) {
                // inactive transaction response is statement error, not tx error
                return;
            }
            if(static_cast<std::underlying_type_t<status>>(st) > 0 || st == status::waiting_for_other_transaction) {
                // Warnings should not be passed to this function.
                // Even in that case, they should not be set as transaction error because they don't abort tx.
                return;
            }
            rctx.transaction()->error_info(info);
        }
    }
}

void set_error_impl(
    request_context& rctx,
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st,
    bool append_stacktrace
) {
    set_error_info(rctx, create_error_info_impl(code, message, filepath, position, st, append_stacktrace));
}

}
