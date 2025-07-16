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

#include <takatori/util/string_builder.h>
#include <sharksfin/StatusCode.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>

namespace jogasaki::utils {

#define handle_kvs_errors(rctx, st) jogasaki::utils::handle_kvs_errors_impl(rctx, st, __FILE__, line_number_string) //NOLINT

/**
 * @brief handle generic kvs errors
 * @details this is the utility function to conduct the error handling for kvs. This function covers many errors
 * frequently occurring in manipulating kvs, e.g. serialization failure (early abort).
 * @note this is generic error handling and is not applicable to all error situation. Depending on the
 * function requirement, it should manually handle and make action for the specific errors.
 * @note this function doesn't handle warnings such as status::not_found, status::already_exists.
 * @note this function handle only known kvs errors, so generic unknown errors should be caught outside
 */
void handle_kvs_errors_impl(
    request_context& context,
    status res,
    std::string_view filepath,
    std::string_view position
) noexcept;

}

