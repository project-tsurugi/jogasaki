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

#include <jogasaki/api/statement_handle.h>
#include <jogasaki/error/error_info.h>

#define create_statement_handle_error(handle) jogasaki::utils::create_statement_handle_error_impl((handle), __FILE__, line_number_string) //NOLINT

namespace jogasaki::utils {

/**
 * @brief create error info for invalid statement handle
 */
std::shared_ptr<error::error_info> create_statement_handle_error_impl(
    api::statement_handle prepared,
    std::string_view filepath,
    std::string_view position
) noexcept;

}  // namespace jogasaki::utils
