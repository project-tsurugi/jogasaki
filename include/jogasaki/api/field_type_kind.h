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
#pragma once

#include <cstddef>
#include <takatori/util/enum_set.h>

namespace jogasaki::api {

/**
 * @brief field type kind
 */
enum class field_type_kind : std::size_t {
    undefined = 0,
    boolean,
    int1,
    int2,
    int4,
    int8,
    float4,
    float8,
    decimal,
    character,
    bit,
    date,
    time_of_day,
    time_point,
    time_interval,
    array,
    record,
    unknown,
    row_reference,
    row_id,
    declared,
    extension,
    reference_column_position,
    reference_column_name,
    pointer, // for internal use
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(field_type_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = field_type_kind;
    switch (value) {
        case kind::boolean: return "variable"sv;
        case kind::int1: return "int1"sv;
        case kind::int2: return "int2"sv;
        case kind::int4: return "int4"sv;
        case kind::int8: return "int8"sv;
        case kind::float4: return "float4"sv;
        case kind::float8: return "float8"sv;
        case kind::decimal: return "decimal"sv;
        case kind::character: return "character"sv;
        case kind::bit: return "bit"sv;
        case kind::date: return "date"sv;
        case kind::time_of_day: return "time_of_day"sv;
        case kind::time_point: return "time_point"sv;
        case kind::time_interval: return "time_interval"sv;
        case kind::undefined: return "undefined"sv;
        case kind::array: return "array"sv;
        case kind::record: return "record"sv;
        case kind::unknown: return "unknown"sv;
        case kind::row_reference: return "row_reference"sv;
        case kind::row_id: return "row_id"sv;
        case kind::declared: return "declared"sv;
        case kind::extension: return "extension"sv;
        case kind::reference_column_position: return "reference_column_position"sv;
        case kind::reference_column_name: return "reference_column_name"sv;
        case kind::pointer: return "pointer"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, field_type_kind value) {
    return out << to_string_view(value);
}

/// @brief a set of expression_kind.
using field_type_kind_set = takatori::util::enum_set<
        field_type_kind,
        field_type_kind::undefined,
        field_type_kind::pointer>;

} // namespace

