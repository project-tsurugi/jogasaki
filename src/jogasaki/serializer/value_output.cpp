#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <boost/assert.hpp>

#include <takatori/datetime/date_interval.h>
#include <takatori/datetime/time_interval.h>
#include <takatori/util/basic_bitset_view.h>
#include <takatori/util/basic_buffer_view.h>
#include <takatori/util/exception.h>

#include <jogasaki/serializer/value_output.h>

#include "base128v.h"
#include "details/value_io_constants.h"

namespace jogasaki::serializer {

using namespace details;

using takatori::util::buffer_view;
using takatori::util::const_bitset_view;
using takatori::util::throw_exception;
using takatori::util::buffer_view;

using byte_type = buffer_view::value_type;

[[nodiscard]] static std::size_t buffer_remaining(
        buffer_view::const_iterator position,
        buffer_view::const_iterator end) {
    auto diff = std::distance(position, end);
    if (diff < 0) {
        return 0;
    }
    return static_cast<std::size_t>(diff);
}

static void write_fixed8(
        std::uint64_t value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    BOOST_ASSERT(position < end); // NOLINT
    (void) end;
    *position = static_cast<byte_type>(value);
    ++position;
}

template<class T>
static void write_fixed(
        T value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    for (std::uint64_t i = 1; i <= sizeof(T); ++i) {
        write_fixed8(value >> ((sizeof(T) - i) * 8U), position, end);
    }
}

static void write_bytes(
        byte_type const* data,
        std::size_t count,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    BOOST_ASSERT(position + count <= end); // NOLINT
    (void) end;
    std::memcpy(position, data, count);
    position += count;
}

bool write_end_of_contents(
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1) {
        return false;
    }
    write_fixed8(header_end_of_contents, position, end);
    return true;
}

bool write_null(
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1) {
        return false;
    }
    write_fixed8(header_unknown, position, end);
    return true;
}

bool write_int(
        std::int64_t value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (min_embed_positive_int_value <= value && value <= max_embed_positive_int_value) {
        // embed positive int
        if (buffer_remaining(position, end) < 1) {
            return false;
        }
        write_fixed8(
                static_cast<std::int64_t>(header_embed_positive_int) + value - min_embed_positive_int_value,
                position,
                end);
    } else if (min_embed_negative_int_value <= value && value <= max_embed_negative_int_value) {
        // embed negative int
        if (buffer_remaining(position, end) < 1) {
            return false;
        }
        write_fixed8(
                static_cast<std::int64_t>(header_embed_negative_int) + value - min_embed_negative_int_value,
                position,
                end);
    } else {
        // normal int
        if (buffer_remaining(position, end) < 1 + base128v::size_signed(value)) {
            return false;
        }

        write_fixed8(header_int, position, end);
        base128v::write_signed(value, position, end);
    }
    return true;
}

bool write_float4(
        float value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1 + 4) {
        return false;
    }
    std::uint32_t bits {};
    std::memcpy(&bits, &value, sizeof(bits));

    write_fixed8(header_float4, position, end);
    write_fixed<std::uint32_t>(bits, position, end);
    return true;
}

bool write_float8(
        double value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1 + 8) {
        return false;
    }
    std::uint64_t bits {};
    std::memcpy(&bits, &value, sizeof(bits));

    write_fixed8(header_float8, position, end);
    write_fixed<std::uint64_t>(bits, position, end);
    return true;
}

static bool has_small_coefficient(takatori::decimal::triple value) {
    if (value.coefficient_high() != 0) {
        return false;
    }
    if (value.coefficient_low() <= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
        return true;
    }
    if (value.coefficient_low() == static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::min())
            && value.sign() < 0) {
        return true;
    }
    return false;
}

static std::tuple<std::uint64_t, std::uint64_t, std::size_t> make_signed_coefficient_full(takatori::decimal::triple value) {
    BOOST_ASSERT(!has_small_coefficient(value)); // NOLINT
    std::uint64_t c_hi = value.coefficient_high();
    std::uint64_t c_lo = value.coefficient_low();

    if (value.sign() >= 0) {
        for (std::size_t offset = 0; offset < sizeof(std::uint64_t); ++offset) {
            std::uint64_t octet = (c_hi >> ((sizeof(std::uint64_t) - offset - 1U) * 8U)) & 0xffU;
            if (octet != 0) {
                std::size_t size { sizeof(std::uint64_t) * 2 - offset };
                if ((octet & 0x80U) != 0) {
                    ++size;
                }
                return { c_hi, c_lo, size };
            }
        }
        return { c_hi, c_lo, sizeof(std::uint64_t) + 1 };
    }

    // for negative numbers

    if (value.sign() < 0) {
        c_lo = ~c_lo + 1;
        c_hi = ~c_hi;
        if (c_lo == 0) {
            c_hi += 1; // carry up
        }
    }

    for (std::size_t offset = 0; offset < sizeof(std::uint64_t); ++offset) {
        std::uint64_t octet = (c_hi >> ((sizeof(std::uint64_t) - offset - 1U) * 8U)) & 0xffU;
        if (octet != 0xffU) {
            std::size_t size { sizeof(std::uint64_t) * 2 - offset };
            if ((octet & 0x80U) == 0) {
                ++size;
            }
            return { c_hi, c_lo, size };
        }
    }
    return { c_hi, c_lo, sizeof(std::uint64_t) + 1 };
}

bool write_decimal(
        takatori::decimal::triple value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    // small coefficient
    if (has_small_coefficient(value)) {
        auto coefficient = static_cast<std::int64_t>(value.coefficient_low());
        if (value.sign() < 0) {
            coefficient = -coefficient;
        }

        // just write as int if exponent is just 0
        if (value.exponent() == 0) {
            return write_int(coefficient, position, end);
        }

        // write compact decimal
        if (buffer_remaining(position, end) < 1
                + base128v::size_signed(value.exponent())
                + base128v::size_signed(coefficient)) {
            return false;
        }
        write_fixed8(header_decimal_compact, position, end);
        base128v::write_signed(value.exponent(), position, end);
        base128v::write_signed(coefficient, position, end);
        return true;
    }

    // for large coefficient
    auto [c_hi, c_lo, c_size] = make_signed_coefficient_full(value);
    BOOST_ASSERT(c_size > sizeof(std::uint64_t)); // NOLINT
    BOOST_ASSERT(c_size <= sizeof(std::uint64_t) * 2 + 1); // NOLINT

    if (buffer_remaining(position, end) < 1
            + base128v::size_signed(value.exponent())
            + base128v::size_unsigned(c_size)
            + c_size) {
        return false;
    }

    write_fixed8(header_decimal, position, end);
    base128v::write_signed(value.exponent(), position, end);
    base128v::write_unsigned(c_size, position, end);

    if (c_size > sizeof(std::uint64_t) * 2) {
        // write sign bit
        if (value.sign() >= 0) {
            write_fixed8(0, position, end);
        } else {
            write_fixed8(0xffU, position, end);
        }
        --c_size;
    }

    // write octets of coefficient
    for (std::size_t offset = 0; offset < sizeof(std::uint64_t); ++offset) {
        *(position + c_size - offset - 1) = static_cast<char>(c_lo >> (offset * 8U));
    }
    for (std::size_t offset = 0; offset < c_size - sizeof(std::uint64_t); ++offset) {
        *(position + c_size - offset - sizeof(std::uint64_t) - 1) = static_cast<char>(c_hi >> (offset * 8U));
    }
    position += c_size;
    return true;
}

bool write_character(
        std::string_view value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    auto size = value.size();

    if (min_embed_character_size <= size && size <= max_embed_character_size) {
        // for short character string
        if (buffer_remaining(position, end) < 1 + size) {
            return false;
        }
        write_fixed8(
                static_cast<std::int64_t>(header_embed_character) + size - min_embed_character_size,
                position,
                end);
    } else {
        // for long character string
        if (buffer_remaining(position, end) < 1 + base128v::size_unsigned(size) + size) {
            return false;
        }
        write_fixed8(header_character, position, end);
        base128v::write_unsigned(size, position, end);
    }
    write_bytes(value.data(), value.size(), position, end);
    return true;
}

bool write_octet(
        std::string_view value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    auto size = value.size();

    if (min_embed_octet_size <= size && size <= max_embed_octet_size) {
        // for short octet string
        if (buffer_remaining(position, end) < 1 + size) {
            return false;
        }
        write_fixed8(
                static_cast<std::int64_t>(header_embed_octet) + size - min_embed_octet_size,
                position,
                end);
    } else {
        // for long octet string
        if (buffer_remaining(position, end) < 1 + base128v::size_unsigned(size) + size) {
            return false;
        }
        write_fixed8(header_octet, position, end);
        base128v::write_unsigned(size, position, end);
    }
    write_bytes(value.data(), value.size(), position, end);
    return true;
}

bool write_bit(
        const_bitset_view value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    auto bit_size = value.size();
    auto byte_size = value.block_size();

    if (min_embed_bit_size <= bit_size && bit_size <= max_embed_bit_size) {
        // for short bit string
        if (buffer_remaining(position, end) < 1 + byte_size) {
            return false;
        }
        write_fixed8(
                static_cast<std::int64_t>(header_embed_bit) + bit_size - min_embed_bit_size,
                position,
                end);
    } else {
        // for long bit string
        if (buffer_remaining(position, end) < 1 + base128v::size_unsigned(bit_size) + byte_size) {
            return false;
        }
        write_fixed8(header_bit, position, end);
        base128v::write_unsigned(bit_size, position, end);
    }
    auto rest_bits = value.size() % 8;
    if (rest_bits == 0) {
        // write all blocks
        write_bytes(value.block_data(), value.block_size(), position, end);
    } else {
        // write blocks except the last
        write_bytes(value.block_data(), value.block_size() - 1, position, end);

        auto last = static_cast<std::uint8_t>(*--value.end());
        write_fixed8(last & ~(0xffU << rest_bits), position, end);
    }
    return true;
}

bool write_bit(
        std::string_view blocks,
        std::size_t number_of_bits,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (number_of_bits > blocks.size() * 8) {
        throw_exception(std::out_of_range("too large number of bits"));
    }
    const_bitset_view bits { blocks.data(), number_of_bits };  //NOLINT(bugprone-suspicious-stringview-data-usage)
    BOOST_ASSERT(bits.block_size() <= blocks.size()); // NOLINT
    return write_bit(bits, position, end);
}

bool write_date(
        takatori::datetime::date value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1 + base128v::size_signed(value.days_since_epoch())) {
        return false;
    }
    write_fixed8(header_date, position, end);
    base128v::write_signed(value.days_since_epoch(), position, end);
    return true;
}

bool write_time_of_day(
        takatori::datetime::time_of_day value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1 + base128v::size_unsigned(value.time_since_epoch().count())) {
        return false;
    }
    write_fixed8(header_time_of_day, position, end);
    base128v::write_unsigned(value.time_since_epoch().count(), position, end);
    return true;
}

bool write_time_of_day_with_offset(
    takatori::datetime::time_of_day value,
    std::int32_t timezone_offset,
    buffer_view::iterator& position,
    buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1 + base128v::size_unsigned(value.time_since_epoch().count())
        + base128v::size_signed(timezone_offset)) {
        return false;
    }
    write_fixed8(header_time_of_day_with_offset, position, end);
    base128v::write_unsigned(value.time_since_epoch().count(), position, end);
    base128v::write_signed(timezone_offset, position, end);
    return true;
}

bool write_time_point(
        takatori::datetime::time_point value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1
            + base128v::size_signed(value.seconds_since_epoch().count())
            + base128v::size_unsigned(value.subsecond().count())) {
        return false;
    }
    write_fixed8(header_time_point, position, end);
    base128v::write_signed(value.seconds_since_epoch().count(), position, end);
    base128v::write_unsigned(value.subsecond().count(), position, end);
    return true;
}

bool write_time_point_with_offset(
    takatori::datetime::time_point value,
    std::int32_t timezone_offset,
    buffer_view::iterator& position,
    buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1
        + base128v::size_signed(value.seconds_since_epoch().count())
        + base128v::size_unsigned(value.subsecond().count())
        + base128v::size_signed(timezone_offset)) {
        return false;
    }
    write_fixed8(header_time_point_with_offset, position, end);
    base128v::write_signed(value.seconds_since_epoch().count(), position, end);
    base128v::write_unsigned(value.subsecond().count(), position, end);
    base128v::write_signed(timezone_offset, position, end);
    return true;
}

bool write_datetime_interval(
        takatori::datetime::datetime_interval value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1
            + base128v::size_signed(value.date().year())
            + base128v::size_signed(value.date().month())
            + base128v::size_signed(value.date().day())
            + base128v::size_signed(value.time().offset().count())) {
        return false;
    }
    write_fixed8(header_datetime_interval, position, end);
    base128v::write_signed(value.date().year(), position, end);
    base128v::write_signed(value.date().month(), position, end);
    base128v::write_signed(value.date().day(), position, end);
    base128v::write_signed(value.time().offset().count(), position, end);
    return true;
}

bool write_array_begin(
        std::size_t size,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (min_embed_array_size <= size && size <= max_embed_array_size) {
        // for short array
        if (buffer_remaining(position, end) < 1) {
            return false;
        }
        write_fixed8(
                static_cast<std::int64_t>(header_embed_array) + size - min_embed_array_size,
                position,
                end);
    } else {
        // for long array
        if (buffer_remaining(position, end) < 1 + base128v::size_unsigned(size)) {
            return false;
        }
        write_fixed8(header_array, position, end);
        base128v::write_unsigned(size, position, end);
    }
    return true;
}

bool write_row_begin(
        std::size_t size,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (min_embed_row_size <= size && size <= max_embed_row_size) {
        // for short row
        if (buffer_remaining(position, end) < 1) {
            return false;
        }
        write_fixed8(
                static_cast<std::int64_t>(header_embed_row) + size - min_embed_row_size,
                position,
                end);
    } else {
        // for long row
        if (buffer_remaining(position, end) < 1 + base128v::size_unsigned(size)) {
            return false;
        }
        write_fixed8(header_row, position, end);
        base128v::write_unsigned(size, position, end);
    }
    return true;
}

bool write_blob(
        std::uint64_t provider,
        std::uint64_t object_id,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1
            + sizeof(std::uint64_t)
            + sizeof(std::uint64_t)) {
        return false;
    }
    write_fixed8(header_blob, position, end);
    write_fixed(provider, position, end);
    write_fixed(object_id, position, end);
    return true;
}

bool write_clob(
        std::uint64_t provider,
        std::uint64_t object_id,
        buffer_view::iterator& position,
        buffer_view::const_iterator end) {
    if (buffer_remaining(position, end) < 1
            + sizeof(std::uint64_t)
            + sizeof(std::uint64_t)) {
        return false;
    }
    write_fixed8(header_clob, position, end);
    write_fixed(provider, position, end);
    write_fixed(object_id, position, end);
    return true;
}

} // namespace jogasaki::serializer
