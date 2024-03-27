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
#include "fail.h"

#include <initializer_list>
#include <stdexcept>
#include <utility>
#include <vector>
#include <ostream>

#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/utils/base_filename.h>

namespace jogasaki::utils {

using takatori::util::string_builder;
using takatori::util::throw_exception;

void fail_with_exception_impl(bool to_throw, std::string_view msg, std::string_view filepath, std::string_view position) {
    string_builder sb{};
    sb << "fatal internal error at " << filepath << ":" << position << " " << msg;
    std::endl(sb.buffer());
    sb << ::boost::stacktrace::stacktrace{};
    auto m = sb << string_builder::to_string;
    LOG_LP(ERROR) << m;
    if(to_throw) {
        throw_exception(std::logic_error{m});
    }
}

}  // namespace jogasaki::utils
