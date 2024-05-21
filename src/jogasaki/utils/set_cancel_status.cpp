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
#include "set_cancel_status.h"

#include <string_view>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>

namespace jogasaki::utils {

void set_cancel_status_impl(
    request_context& context,
    std::string_view filepath,
    std::string_view position
) noexcept {
    error::set_error_impl(
        context,
        error_code::request_canceled,
        "the operation has been canceled",
        filepath,
        position,
        status::request_canceled,
        false
    );
}

}  // namespace jogasaki::utils
