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

#include <iomanip>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <jogasaki/request_context.h>
#include <jogasaki/request_cancel_config.h>

#define cancel_request(rctx) jogasaki::utils::cancel_request_impl(rctx, __FILE__, line_number_string) //NOLINT

namespace jogasaki::utils {

/**
 * @brief set cancel status in request/transaction context
 * @details set cancel error info or status to the request context, and try abort if transaction is involved in the request
 */
void cancel_request_impl(
    request_context& context,
    std::string_view filepath,
    std::string_view position
);

/**
 * @brief accessor to the flag on whether the request cancel kind is enabled
 * @param kind cancel kind to query
 * @return true if the cancel kind is enabled
 * @return false otherwise
 */
bool request_cancel_enabled(request_cancel_kind kind) noexcept;

}  // namespace jogasaki::utils
