#pragma once

#include <cstdint>
#include <cstdlib>

#include <ostream>
#include <string_view>

namespace jogasaki::serializer {

/// @brief represents a kind of serialized entry.
enum class entry_type {

    /// @brief represents end of contents.
    end_of_contents,

    /// @brief represents value is absent.
    null,

    /**
     * @brief 64-bit signed integer.
     * @note this represents {@code int4}, {@code int8} or {@code boolean}.
     */
    int_,

    /// @brief fixed 32-bit floating point number.
    float4,

    /// @brief fixed 64-bit floating point number.
    float8,

    /// @brief decimal number.
    decimal,

    /// @brief variable length character sequence.
    character,

    /// @brief variable length octet sequence.
    octet,

    /// @brief variable length bit sequence.
    bit,

    /// @brief date value.
    date,

    /// @brief time of day value.
    time_of_day,

    /// @brief time of day with offset value.
    time_of_day_with_offset,

    /// @brief time-point value.
    time_point,

    /// @brief time-point with offset value.
    time_point_with_offset,

    /// @brief date-time interval value.
    datetime_interval,

    /// @brief rows.
    row,

    /// @brief arrays.
    array,

    /// @brief character large objects.
    clob,

    /// @brief binary large objects.
    blob,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
constexpr inline std::string_view to_string_view(entry_type value) noexcept {
    using namespace std::string_view_literals;
    using kind = entry_type;
    switch (value) {
        case kind::end_of_contents: return "end_of_contents"sv;
        case kind::null: return "null"sv;
        case kind::int_: return "int_"sv;
        case kind::float4: return "float4"sv;
        case kind::float8: return "float8"sv;
        case kind::decimal: return "decimal"sv;
        case kind::character: return "character"sv;
        case kind::octet: return "octet"sv;
        case kind::bit: return "bit"sv;
        case kind::date: return "date"sv;
        case kind::time_of_day: return "time_of_day"sv;
        case kind::time_of_day_with_offset: return "time_of_day_with_offset"sv;
        case kind::time_point: return "time_point"sv;
        case kind::time_point_with_offset: return "time_point_with_offset"sv;
        case kind::datetime_interval: return "datetime_interval"sv;
        case kind::row: return "row"sv;
        case kind::array: return "array"sv;
        case kind::clob: return "clob"sv;
        case kind::blob: return "blob"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, entry_type value) {
    return out << to_string_view(value);
}

} // namespace jogasaki::serializer
