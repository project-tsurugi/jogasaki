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
#include <ostream>
#include <stdexcept>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/utils/base_filename.h>

namespace jogasaki::utils {

using takatori::util::string_builder;
using takatori::util::throw_exception;

std::string create_fatal_msg(std::string_view msg, std::string_view filepath, std::string_view position) {
    string_builder sb{};
    sb << "fatal internal error at " << filepath << ":" << position << " " << msg;
    std::endl(sb.buffer());
    sb << ::boost::stacktrace::stacktrace{};
    return sb << string_builder::to_string;
}

void fail_with_exception_impl(std::string_view msg, std::string_view filepath, std::string_view position) {
    auto m = create_fatal_msg(msg, filepath, position);
    LOG_LP(ERROR) << m;
    throw_exception(std::logic_error{m});
}

void fail_no_exception_impl(std::string_view msg, std::string_view filepath, std::string_view position) {
    auto m = create_fatal_msg(msg, filepath, position);
    LOG_LP(ERROR) << m;
}

}  // namespace jogasaki::utils
