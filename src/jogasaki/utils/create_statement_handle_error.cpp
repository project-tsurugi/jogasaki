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
#include "create_statement_handle_error.h"

#include <takatori/util/string_builder.h>

#include <jogasaki/error/error_info_factory.h>

namespace jogasaki::utils {

using takatori::util::string_builder;

std::shared_ptr<error::error_info> create_statement_handle_error_impl(
    api::statement_handle prepared,
    std::string_view filepath,
    std::string_view position
) noexcept {
    return jogasaki::error::create_error_info_impl(
        error_code::statement_not_found_exception,
        string_builder{} << "prepared statement not found " << prepared << string_builder::to_string,
        filepath,
        position,
        status::err_invalid_argument,
        false
    );
}

}  // namespace jogasaki::utils
