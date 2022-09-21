#include <jogasaki/serializer/value_input.h>

#include <algorithm>
#include <optional>
#include <stdexcept>

#include "base128v.h"
#include "details/value_io_constants.h"

#include <takatori/util/assertion.h>
#include <takatori/util/exception.h>
#include <takatori/util/fail.h>
#include <takatori/util/string_builder.h>

namespace jogasaki::serializer {

using namespace details;

using takatori::util::buffer_view;
using takatori::util::const_buffer_view;
using takatori::util::const_bitset_view;

using byte_type = buffer_view::value_type;

static void requires_entry(
        entry_type expect,
        buffer_view::const_iterator position,
        buffer_view::const_iterator end) {
    if (auto ret = peek_type(position, end); ret != expect) {
        using ::takatori::util::string_builder;
        using ::takatori::util::throw_exception;
        throw_exception(std::runtime_error(string_builder {}
                << "inconsistent entry type: "
                << "retrieved '" << ret << "', "
                << "but expected is '" << expect << "'"
                << string_builder::to_string));
    }
}

template<class T>
static std::optional<T> extract(
        buffer_view::value_type first,
        std::uint32_t header,
        std::uint32_t mask,
        T min_value) {
    auto unsigned_value = static_cast<unsigned char>(first);
    if (header <= unsigned_value && unsigned_value <= header + mask) {
        return { static_cast<T>(unsigned_value - header) + min_value };
    }
    return std::nullopt;
}

static std::int64_t read_sint(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    if (auto result = base128v::read_signed(position, end)) {
        return *result;
    }
    throw_buffer_underflow();
}

static std::uint64_t read_uint(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    if (auto result = base128v::read_unsigned(position, end)) {
        return *result;
    }
    throw_buffer_underflow();
}

static std::int32_t read_sint32(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    auto value = read_sint(position, end);
    if (value < std::numeric_limits<std::int32_t>::min() || value > std::numeric_limits<std::int32_t>::max()) {
        throw_int32_value_out_of_range(value);
    }
    return static_cast<std::int32_t>(value);
}

static std::uint32_t read_size(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    auto size = read_uint(position, end);
    if (size >= limit_size) {
        throw_size_out_of_range(size, limit_size);
    }
    return static_cast<std::uint32_t>(size);
}

static const_buffer_view read_bytes(
        std::size_t size,
        buffer_view::const_iterator& position,
        buffer_view::const_iterator end) {
    if (std::distance(position, end) < static_cast<buffer_view::difference_type>(size)) {
        throw_buffer_underflow();
    }
    const_buffer_view result { position, size };
    position += size;
    return result;
}

template<class T>
static T read_fixed(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    static_assert(std::is_integral_v<T>);
    static_assert(std::is_unsigned_v<T>);
    if (std::distance(position, end) < static_cast<buffer_view::difference_type>(sizeof(T))) {
        throw_buffer_underflow();
    }
    T result { 0 };
    for (std::size_t i = 1; i <= sizeof(T); ++i) {
        T value { static_cast<unsigned char>(*position) };
        result |= value << ((sizeof(T) - i) * 8U);
        ++position;
    }
    return result;
}

void read_end_of_contents(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::end_of_contents, position, end);
    ++position;
}

void read_null(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::null, position, end);
    ++position;
}

entry_type peek_type(buffer_view::const_iterator position, buffer_view::const_iterator end) {
    if (position == end) {
        throw_buffer_underflow();
    }
    std::uint32_t head = static_cast<unsigned char>(*position);
    if (head <= header_embed_positive_int + mask_embed_positive_int) {
        return entry_type::int_;
    }
    static_assert(header_embed_positive_int + mask_embed_positive_int + 1 == header_embed_character);
    if (head <= header_embed_character + mask_embed_character) {
        return entry_type::character;
    }
    static_assert(header_embed_character + mask_embed_character + 1 == header_embed_row);
    if (head <= header_embed_row + mask_embed_row) {
        return entry_type::row;
    }
    static_assert(header_embed_row + mask_embed_row + 1 == header_embed_array);
    if (head <= header_embed_array + mask_embed_array) {
        return entry_type::array;
    }
    static_assert(header_embed_array + mask_embed_array + 1 == header_embed_negative_int);
    if (head <= header_embed_negative_int + mask_embed_negative_int) {
        return entry_type::int_;
    }
    static_assert(header_embed_negative_int + mask_embed_negative_int + 1 == header_embed_octet);
    if (head <= header_embed_octet + mask_embed_octet) {
        return entry_type::octet;
    }
    static_assert(header_embed_bit + mask_embed_bit + 1 == header_unknown);
    if (head <= header_embed_bit + mask_embed_bit) {
        return entry_type::bit;
    }
    switch (head) {
        case header_int: return entry_type::int_;
        case header_float4: return entry_type::float4;
        case header_float8: return entry_type::float8;
        case header_decimal_compact: return entry_type::decimal;
        case header_decimal: return entry_type::decimal;
        case header_character: return entry_type::character;
        case header_octet: return entry_type::octet;
        case header_bit: return entry_type::bit;
        case header_date: return entry_type::date;
        case header_time_of_day: return entry_type::time_of_day;
        case header_time_point: return entry_type::time_point;
        case header_datetime_interval: return entry_type::datetime_interval;
        case header_row: return entry_type::row;
        case header_array: return entry_type::array;
        case header_clob: return entry_type::clob;
        case header_blob: return entry_type::blob;
        case header_end_of_contents: return entry_type::end_of_contents;
        case header_unknown: return entry_type::null;

        default:
            throw_unrecognized_entry(static_cast<unsigned char>(head));
    }
}

std::int64_t read_int(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::int_, position, end);
    auto first = *position;
    if (auto value = extract(
            first,
            header_embed_positive_int,
            mask_embed_positive_int,
            min_embed_positive_int_value)) {
        ++position;
        return *value;
    }
    if (auto value = extract(
            first,
            header_embed_negative_int,
            mask_embed_negative_int,
            min_embed_negative_int_value)) {
        ++position;
        return *value;
    }

    BOOST_ASSERT(static_cast<unsigned char>(first) == header_int); // NOLINT
    buffer_view::const_iterator iter = position;
    ++iter;
    auto result = read_sint(iter, end);
    position = iter;
    return result;
}

float read_float4(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::float4, position, end);
    buffer_view::const_iterator iter = position;
    ++iter;

    auto bits = read_fixed<std::uint32_t>(iter, end);
    float result {};
    std::memcpy(&result, &bits, sizeof(result));
    position = iter;
    return result;
}

double read_float8(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::float8, position, end);
    buffer_view::const_iterator iter = position;
    ++iter;

    auto bits = read_fixed<std::uint64_t>(iter, end);
    double result {};
    std::memcpy(&result, &bits, sizeof(result));
    position = iter;
    return result;
}

const_buffer_view read_decimal_coefficient(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    auto size = read_uint(position, end);
    if (size == 0 || size > max_decimal_coefficient_size) {
        throw_decimal_coefficient_out_of_range(size);
    }
    auto bytes = read_bytes(size, position, end);
    if (size != max_decimal_coefficient_size) {
        return bytes;
    }

    auto first = static_cast<std::uint8_t>(bytes[0]);
    // positive is OK because coefficient is [0, 2^128)
    if (first == 0) {
        return bytes;
    }

    if (first == 0xffU) {
        // check negative value to avoid -2^128 (0xff 0x00.. 0x00)
        auto const* found = std::find_if(
                bytes.begin() + 1,
                bytes.end(),
                [](auto c) { return c != '\0'; });
        if (found != bytes.end()) {
            return bytes;
        }
    }

    throw_decimal_coefficient_out_of_range(size);
}

takatori::decimal::triple read_decimal(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    // int encoded
    if (peek_type(position, end) == entry_type::int_) {
        auto value = read_int(position, end);
        return takatori::decimal::triple { value, 0 };
    }

    // decimal encoded
    requires_entry(entry_type::decimal, position, end);

    buffer_view::const_iterator iter = position;
    auto first = static_cast<unsigned char>(*iter);
    ++iter;

    // compact decimal value
    if (first == header_decimal_compact) {
        auto exponent = read_sint32(iter, end);
        auto coefficient = read_sint(iter, end);
        position = iter;
        return { coefficient, exponent };
    }

    // full decimal value
    BOOST_ASSERT(first == header_decimal); // NOLINT

    auto exponent = read_sint32(iter, end);
    auto coefficient = read_decimal_coefficient(iter, end);

    // extract lower 8-octets of coefficient
    std::uint64_t c_lo {};
    std::uint64_t shift {};
    for (
            std::size_t offset = 0;
            offset < coefficient.size() && offset < sizeof(std::uint64_t);
            ++offset) {
        auto pos = coefficient.size() - offset - 1;
        std::uint64_t octet { static_cast<std::uint8_t>(coefficient[pos]) };
        c_lo |= octet << shift;
        shift += 8;
    }

    // extract upper 8-octets of coefficient
    std::uint64_t c_hi {};
    shift = 0;
    for (
            std::size_t offset = sizeof(std::uint64_t);
            offset < coefficient.size() && offset < sizeof(std::uint64_t) * 2;
            ++offset) {
        auto pos = coefficient.size() - offset - 1;
        std::uint64_t octet { static_cast<std::uint8_t>(coefficient[pos]) };
        c_hi |= octet << shift;
        shift += 8;
    }

    bool negative = (static_cast<std::uint8_t>(coefficient[0]) & 0x80U) != 0;

    if (negative) {
        // sign extension
        if (coefficient.size() < sizeof(std::uint64_t) * 2) {
            BOOST_ASSERT(coefficient.size() >= sizeof(std::uint64_t)); // NOLINT
            auto mask = std::numeric_limits<std::uint64_t>::max(); // 0xfff.....
            std::size_t rest = (coefficient.size() - sizeof(std::uint64_t)) * 8U;
            c_hi |= mask << rest;
        }

        c_lo = ~c_lo + 1;
        c_hi = ~c_hi;
        if (c_lo == 0) {
            c_hi += 1; // carry up
        }
        // if negative, coefficient must not be zero
        BOOST_ASSERT(c_lo != 0 || c_hi != 0); // NOLINT
    }

    position = iter;
    return takatori::decimal::triple {
        negative ? -1 : +1,
        c_hi,
        c_lo,
        exponent,
    };
}

std::string_view read_character(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::character, position, end);
    buffer_view::const_iterator iter = position;
    std::size_t size {};
    auto first = *iter;
    if (auto value = extract(
            first,
            header_embed_character,
            mask_embed_character,
            min_embed_character_size)) {
        size = *value;
        ++iter;
    } else {
        BOOST_ASSERT(static_cast<unsigned char>(first) == header_character); // NOLINT
        ++iter;
        size = read_size(iter, end);
    }

    auto result = read_bytes(size, iter, end);
    position = iter;
    return { result.data(), result.size() };
}

std::string_view read_octet(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::octet, position, end);
    buffer_view::const_iterator iter = position;
    std::size_t size {};
    auto first = *iter;
    if (auto value = extract(
            first,
            header_embed_octet,
            mask_embed_octet,
            min_embed_octet_size)) {
        size = *value;
        ++iter;
    } else {
        BOOST_ASSERT(static_cast<unsigned char>(first) == header_octet); // NOLINT
        ++iter;
        size = read_size(iter, end);
    }

    auto result = read_bytes(size, iter, end);
    position = iter;
    return { result.data(), result.size() };
}

const_bitset_view read_bit(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::bit, position, end);
    buffer_view::const_iterator iter = position;
    std::size_t size {};
    auto first = *iter;
    if (auto value = extract(
            first,
            header_embed_bit,
            mask_embed_bit,
            min_embed_bit_size)) {
        size = *value;
        ++iter;
    } else {
        BOOST_ASSERT(static_cast<unsigned char>(first) == header_bit); // NOLINT
        ++iter;
        size = read_size(iter, end);
    }

    std::size_t block_size = (size + 7) / 8;
    auto result = read_bytes(block_size, iter, end);
    position = iter;
    return { result.data(), size };
}

takatori::datetime::date read_date(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::date, position, end);
    buffer_view::const_iterator iter = position;
    ++iter;
    auto offset = read_sint(iter, end);
    position = iter;
    return takatori::datetime::date { offset };
}

takatori::datetime::time_of_day read_time_of_day(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::time_of_day, position, end);
    buffer_view::const_iterator iter = position;
    ++iter;
    auto offset = read_uint(iter, end);
    position = iter;
    return takatori::datetime::time_of_day { takatori::datetime::time_of_day::time_unit { offset }};
}

takatori::datetime::time_point read_time_point(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::time_point, position, end);
    buffer_view::const_iterator iter = position;
    ++iter;
    auto offset = read_sint(iter, end);
    auto adjustment = read_uint(iter, end);
    position = iter;
    return takatori::datetime::time_point {
            takatori::datetime::time_point::offset_type { offset },
            takatori::datetime::time_point::subsecond_unit { adjustment },
    };
}

takatori::datetime::datetime_interval read_datetime_interval(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::datetime_interval, position, end);
    buffer_view::const_iterator iter = position;
    ++iter;
    auto year = read_sint32(iter, end);
    auto month = read_sint32(iter, end);
    auto day = read_sint32(iter, end);
    auto time = read_sint(iter, end);
    position = iter;
    return takatori::datetime::datetime_interval {
            { year, month, day },
            takatori::datetime::time_interval { takatori::datetime::time_interval::time_unit { time } },
    };
}

std::size_t read_array_begin(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::array, position, end);
    auto first = *position;
    if (auto size = extract(
            first,
            header_embed_array,
            mask_embed_array,
            min_embed_array_size)) {
        ++position;
        return *size;
    }

    BOOST_ASSERT(static_cast<unsigned char>(first) == header_array); // NOLINT
    buffer_view::const_iterator iter = position;
    ++iter;
    auto size = read_size(iter, end);
    position = iter;
    return size;
}

std::size_t read_row_begin(buffer_view::const_iterator& position, buffer_view::const_iterator end) {
    requires_entry(entry_type::row, position, end);
    auto first = *position;
    if (auto size = extract(
            first,
            header_embed_row,
            mask_embed_row,
            min_embed_row_size)) {
        ++position;
        return *size;
    }

    BOOST_ASSERT(static_cast<unsigned char>(first) == header_row); // NOLINT
    buffer_view::const_iterator iter = position;
    ++iter;
    auto size = read_size(iter, end);
    position = iter;
    return size;
}

// FIXME: impl blob, clob

} // namespace jogasaki::serializer
