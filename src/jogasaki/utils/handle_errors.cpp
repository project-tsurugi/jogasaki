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
#include "handle_errors.h"

#include <takatori/util/string_builder.h>

#include <jogasaki/error/error_info_factory.h>

namespace jogasaki::utils {

using takatori::util::string_builder;

void handle_errors_impl(
    request_context& context,
    status res,
    std::string_view filepath,
    std::string_view position
) noexcept {
    if(res == status::ok) return;
    if(res == status::err_serialization_failure) {
        error::set_error_impl(
            context,
            error_code::cc_exception,
            string_builder{} <<
                "Serialization failed. " << string_builder::to_string,
            filepath,
            position,
            res
        );
        return;
    }
    if(res == status::err_conflict_on_write_preserve) {
        error::set_error_impl(
            context,
            error_code::conflict_on_write_preserve_exception,
            string_builder{} <<
                "Serialization failed due to conflict on write preserve. " << string_builder::to_string,
            filepath,
            position,
            res
        );
        return;
    }
    error::set_error_impl(
        context,
        error_code::sql_service_exception,
        string_builder{} <<
            "Unexpected error occurred. status:" << res << string_builder::to_string,
        filepath,
        position,
        res
    );
}
}

