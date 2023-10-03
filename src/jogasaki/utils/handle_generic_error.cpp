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
#include "handle_kvs_errors.h"

#include <takatori/util/string_builder.h>

namespace jogasaki::utils {

using takatori::util::string_builder;

void handle_generic_error_impl(
    request_context& context,
    status res,
    error_code ec,
    std::string_view filepath,
    std::string_view position
) noexcept {
    if(! context.error_info()) {
        error::set_error_impl(
            context,
            ec,
            string_builder{} << "Unexpected error occurred. status:" << res << string_builder::to_string,
            filepath,
            position,
            res,
            true
        );
    }

}
}

