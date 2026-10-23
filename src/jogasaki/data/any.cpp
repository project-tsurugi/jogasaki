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
#include "any.h"

namespace jogasaki::data {

any::operator bool() const noexcept {
    return !empty() && !error();
}

bool any::empty() const noexcept {
    return body_.index() == 0;
}

bool any::error() const noexcept {
    return body_.index() == 1;
}

std::size_t any::type_index() const noexcept {
    return body_.index();
}

bool operator==(any const& a, any const& b) noexcept { return a.body_ == b.body_; }

std::ostream& operator<<(std::ostream& out, any const& value) {
    out << "any(index:" << value.type_index() << ", ";
    value.visit([&](auto const& item) {
        if constexpr (std::is_same_v<std::decay_t<decltype(item)>, std::monostate>) out << "[empty]";
        else out << item;
    });
    return out << ")";
}

std::string_view type_name(any const& value) {
    constexpr std::string_view names[]{"empty", "error", "int1", "int2", "int4", "int8", "float4", "float8",
        "character", "octet", "decimal", "date", "time_of_day", "time_point", "blob", "clob", "column_position"};
    static_assert(std::size(names) == std::variant_size_v<any::base_type>);
    return names[value.type_index()];
}
}


