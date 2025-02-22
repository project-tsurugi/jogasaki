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

#include "key_range.h"

#include <iomanip>
#include <iostream>
#include <optional>
#include <string>

namespace jogasaki::dist {

std::string_view key_range::begin_key() const noexcept { return begin_key_; }

kvs::end_point_kind key_range::begin_endpoint() const noexcept { return begin_endpoint_; }

std::string_view key_range::end_key() const noexcept { return end_key_; }

kvs::end_point_kind key_range::end_endpoint() const noexcept { return end_endpoint_; }

void key_range::dump(std::ostream& out, int indent) const noexcept {
    std::string indent_space(indent, ' ');
    auto hex_dump = [&](const std::string_view& key) {
        std::ostringstream oss;
        for (unsigned char c : key) {
            oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(c) << " ";
        }
        return oss.str();
    };
    out << indent_space << "  begin_endpoint_: " << hex_dump(begin_key_) << "\n";
    out << indent_space << "  end_endpoint_: " << hex_dump(end_key_) << "\n";
}

} // namespace jogasaki::dist
