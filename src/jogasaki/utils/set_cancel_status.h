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

#include <jogasaki/request_context.h>

namespace jogasaki::utils {

#define set_cancel_status(rctx) jogasaki::utils::set_cancel_status_impl(rctx, __FILE__, line_number_string) //NOLINT

/**
 * @brief set cancel error info or status to the request context
 * @details this is the utility function to set the internal errror when cancel request is conducted
 */
void set_cancel_status_impl(
    request_context& context,
    std::string_view filepath,
    std::string_view position
) noexcept;

}  // namespace jogasaki::utils
