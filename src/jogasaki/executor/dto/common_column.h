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
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <variant>

namespace jogasaki::executor::dto {

/**
 * @brief lightweight representation of protocol buffer common.Column message.
 */
struct common_column {
    /**
     * @brief atom type
     */
    enum class atom_type {
        type_unspecified = 0,
        boolean = 1,
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
            case atom_type::type_unspecified: return "type_unspecified"sv;
            case atom_type::boolean: return "boolean"sv;
            case atom_type::int4: return "int4"sv;
            case atom_type::int8: return "int8"sv;
            case atom_type::float4: return "float4"sv;
            case atom_type::float8: return "float8"sv;
            case atom_type::decimal: return "decimal"sv;
            case atom_type::character: return "character"sv;
            case atom_type::octet: return "octet"sv;
            case atom_type::bit: return "bit"sv;
            case atom_type::date: return "date"sv;
            case atom_type::time_of_day: return "time_of_day"sv;
            case atom_type::time_point: return "time_point"sv;
            case atom_type::datetime_interval: return "datetime_interval"sv;
            case atom_type::time_of_day_with_time_zone: return "time_of_day_with_time_zone"sv;
            case atom_type::time_point_with_time_zone: return "time_point_with_time_zone"sv;
            case atom_type::clob: return "clob"sv;
            case atom_type::blob: return "blob"sv;
            case atom_type::unknown: return "unknown"sv;
        }
        std::abort();
    }

    /**
     * @brief stream operator for atom_type.
     */
    friend inline std::ostream& operator<<(std::ostream& out, atom_type value) {
        return out << to_string_view(value);
    }

    /**
     * @brief construct empty object
     */
    common_column() = default;

    /**
     * @brief construct new object (for testing)
     */
    common_column(
        std::string name,
        atom_type type,
        bool nullable,
        std::optional<std::variant<std::uint32_t, bool>> length = std::nullopt,
        std::optional<std::variant<std::uint32_t, bool>> precision = std::nullopt,
        std::optional<std::variant<std::uint32_t, bool>> scale = std::nullopt
    ) :
        name_(std::move(name)),
        atom_type_(type),
        length_(length),
        precision_(precision),
        scale_(scale),
        nullable_(nullable)
    {}

    std::string name_{};
    atom_type atom_type_{atom_type::type_unspecified};
    std::uint32_t dimension_{}; // unused

    // optional length/precision/scale information (bool=true means arbitrary)
    std::optional<std::variant<std::uint32_t, bool>> length_{};
    std::optional<std::variant<std::uint32_t, bool>> precision_{};
    std::optional<std::variant<std::uint32_t, bool>> scale_{};

    std::optional<bool> nullable_{};
    std::optional<bool> varying_{};
    std::optional<std::string> description_{};

    /**
     * @brief equality comparison.
     */
    friend bool operator==(common_column const& a, common_column const& b) {
        return a.name_ == b.name_
            && a.atom_type_ == b.atom_type_
            && a.dimension_ == b.dimension_
            && a.length_ == b.length_
            && a.precision_ == b.precision_
            && a.scale_ == b.scale_
            && a.nullable_ == b.nullable_
            && a.varying_ == b.varying_
            && a.description_ == b.description_;
    }

    /**
     * @brief inequality comparison.
     */
    friend bool operator!=(common_column const& a, common_column const& b) {
        return !(a == b);
    }

    /**
     * @brief stream operator for common_column
     */
    friend inline std::ostream& operator<<(std::ostream& out, common_column const& v) {
        out << "common_column{name:\"" << v.name_ << "\"";
        out << " type:" << v.atom_type_;

        auto print_opt_variant = [](std::ostream& out, auto const& opt, std::string_view name) {
            if (opt) {
                out << ' ' << name << ':';
                std::visit([&out](auto const& val) {
                    using T = std::decay_t<decltype(val)>;
                    if constexpr (std::is_same_v<T, bool>) {
                        // bool==true indicates arbitrary (print as "*")
                        out << (val ? "*" : "false");
                    } else {
                        out << val;
                    }
                }, *opt);
            }
        };

        print_opt_variant(out, v.length_, "length");
        print_opt_variant(out, v.precision_, "precision");
        print_opt_variant(out, v.scale_, "scale");

        if (v.nullable_) {
            out << " nullable:" << (*v.nullable_ ? "true" : "false");
        }
        if (v.varying_) {
            out << " varying:" << (*v.varying_ ? "true" : "false");
        }
        if (v.description_) {
            out << " desc:\"" << *v.description_ << "\"";
        }
        out << "}";
        return out;
    }

};

} // namespace jogasaki::executor
