/*
 * Copyright 2018-2020 tsurugi project.
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
#include <takatori/util/stacktrace.h>

namespace jogasaki::error {

std::shared_ptr<error_info> create_error_info_impl(
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st
) {
    std::stringstream ss{};
    ss << ::boost::stacktrace::stacktrace{};
    auto info = std::make_shared<error_info>(code, message, filepath, position, ss.str());
    info->status(st);
    return info;
}

void set_error_impl(
    request_context& rctx,
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st
) {
    rctx.error_info(
        create_error_info_impl(code, message, filepath, position, st)
    );
}

void set_tx_error_impl(
    request_context& rctx,
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st
) {
    auto info = create_error_info_impl(code, message, filepath, position, st);
    if(rctx.error_info(info)) {
        if(rctx.transaction()) {
            rctx.transaction()->error_info(std::move(info));
        }
    }
}

}
