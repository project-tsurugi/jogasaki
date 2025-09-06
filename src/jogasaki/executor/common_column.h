/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstdint>
#include <ostream>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace jogasaki::executor {

/**
 * @brief lightweight representation of protocol buffer common.Column message.
 *
 * @details This struct intentionally avoids any inclusion of generated
 * protobuf headers so it can be used in translation units that do not
 * depend on protobuf. Use conversion helpers in
 * `common_column_utils.h` to convert to/from protobuf messages.
 */
struct common_column {
    /**
     * @brief atom type (corresponding to proto AtomType).
     */
    enum class atom_type {
        type_unspecified = 0,
        boolean = 1,
        int1 = 2,
        int2 = 3,
        int4 = 4,
        int8 = 5,
        float4 = 6,
        float8 = 7,
        decimal = 8,
        character = 9,
        octet = 11,
        bit = 13,
        date = 15,
        time_of_day = 16,
        time_point = 17,
        datetime_interval = 18,
        time_of_day_with_time_zone = 19,
        time_point_with_time_zone = 20,
        clob = 21,
        blob = 22,
        unknown = 31,
    };

    /**
     * @brief returns string representation of the value.
     */
    [[nodiscard]] static constexpr inline std::string_view to_string_view(atom_type value) noexcept {
        using namespace std::string_view_literals;
        switch (value) {
            case atom_type::type_unspecified: return "TYPE_UNSPECIFIED"sv;
            case atom_type::boolean: return "BOOLEAN"sv;
            case atom_type::int1: return "INT1"sv;
            case atom_type::int2: return "INT2"sv;
            case atom_type::int4: return "INT4"sv;
            case atom_type::int8: return "INT8"sv;
            case atom_type::float4: return "FLOAT4"sv;
            case atom_type::float8: return "FLOAT8"sv;
            case atom_type::decimal: return "DECIMAL"sv;
            case atom_type::character: return "CHARACTER"sv;
            case atom_type::octet: return "OCTET"sv;
            case atom_type::bit: return "BIT"sv;
            case atom_type::date: return "DATE"sv;
            case atom_type::time_of_day: return "TIME_OF_DAY"sv;
            case atom_type::time_point: return "TIME_POINT"sv;
            case atom_type::datetime_interval: return "DATETIME_INTERVAL"sv;
            case atom_type::time_of_day_with_time_zone: return "TIME_OF_DAY_WITH_TIME_ZONE"sv;
            case atom_type::time_point_with_time_zone: return "TIME_POINT_WITH_TIME_ZONE"sv;
            case atom_type::clob: return "CLOB"sv;
            case atom_type::blob: return "BLOB"sv;
            case atom_type::unknown: return "UNKNOWN"sv;
        }
        std::abort();
    }

    /**
     * @brief stream operator for atom_type.
     */
    friend inline std::ostream& operator<<(std::ostream& out, atom_type value) {
        return out << to_string_view(value);
    }

    // members
    std::string name_{}; ///< optional column name
    atom_type atom_type_{atom_type::type_unspecified};
    std::uint32_t dimension_{}; ///< type dimension for arrays

    // optional length/precision/scale information
    // variant<uint32_t, bool>: uint32_t => defined value, bool(true) => arbitrary
    std::optional<std::variant<std::uint32_t, bool>> length_opt_{};
    std::optional<std::variant<std::uint32_t, bool>> precision_opt_{};
    std::optional<std::variant<std::uint32_t, bool>> scale_opt_{};

    std::optional<bool> nullable_opt_{};
    std::optional<bool> varying_opt_{};
    std::optional<std::string> description_{};

    /**
     * @brief equality comparison.
     */
    friend bool operator==(common_column const& a, common_column const& b) noexcept {
        return a.name_ == b.name_
            && a.atom_type_ == b.atom_type_
            && a.dimension_ == b.dimension_
            && a.length_opt_ == b.length_opt_
            && a.precision_opt_ == b.precision_opt_
            && a.scale_opt_ == b.scale_opt_
            && a.nullable_opt_ == b.nullable_opt_
            && a.varying_opt_ == b.varying_opt_
            && a.description_ == b.description_;
    }

    friend bool operator!=(common_column const& a, common_column const& b) noexcept {
        return !(a == b);
    }
};

} // namespace jogasaki::executor
