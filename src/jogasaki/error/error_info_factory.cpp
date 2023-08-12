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
    code error_code,
    std::string_view message,
    std::string_view filepath,
    std::string_view location
) {
    std::stringstream ss{};
    ss << ::boost::stacktrace::stacktrace{};
    return std::make_shared<error_info>(error_code, message, filepath, location, ss.str());
}

}
