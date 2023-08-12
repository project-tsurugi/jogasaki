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
#include "error_info.h"

#include <string>
#include <string_view>

#include <jogasaki/status.h>

namespace jogasaki::error {

error_info::error_info(
    error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    std::string_view stacks
) noexcept :
    error_code_(code),
    message_(message),
    source_file_path_(filepath),
    source_file_position_(position),
    stacks_(stacks)
{}

void error_info::status(jogasaki::status st) noexcept {
    status_ = st;
}

jogasaki::status error_info::status() const noexcept {
    return status_;
}

std::string_view error_info::message() const noexcept {
    return message_;
}

jogasaki::error_code error_info::code() const noexcept {
    return error_code_;
}

std::string_view error_info::supplemental_text() const noexcept {
    return stacks_;  //FIXME
}

std::string_view error_info::source_file_path() const noexcept {
    return source_file_path_;
}

std::string_view error_info::source_file_position() const noexcept {
    return source_file_position_;
}

}

