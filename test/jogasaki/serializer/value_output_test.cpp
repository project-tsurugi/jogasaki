#include <cstring>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <gtest/gtest.h>

#include <takatori/datetime/date_interval.h>
#include <takatori/datetime/time_interval.h>
#include <takatori/util/basic_bitset_view.h>
#include <takatori/util/basic_buffer_view.h>

#include <jogasaki/serializer/base128v.h>
#include <jogasaki/serializer/details/value_io_constants.h>
#include <jogasaki/serializer/value_output.h>


namespace jogasaki::serializer {

using buffer = takatori::util::buffer_view;
using bitset = takatori::util::bitset_view;

using namespace details;

class value_output_test : public ::testing::Test {
public:
    static std::string bytes(std::initializer_list<std::uint8_t> values) {
        std::string results {};
        results.resize(values.size());
        std::size_t index { 0 };
        for (auto c : values) {
            results[index++] = static_cast<char>(c);
        }
        return results;
    }

    static std::string sequence(std::int64_t header, std::initializer_list<std::string> rest) {
        std::string results {};
        results.append(1, static_cast<char>(header));
        for (auto&& str : rest) {
            results.append(str);
        }
        return results;
    }

    static std::string perform(
            std::function<bool(buffer::iterator&, buffer::iterator)> const& action,
            std::size_t buffer_size = 256) {
        std::string results {};
        results.resize(buffer_size);
        buffer buf { results.data(), results.size() };
        buffer::iterator iter = buf.begin();
        if (!action(iter, buf.end())) {
            throw std::runtime_error("underflow");
        }
        results.resize(std::distance(buf.begin(), iter));
        return results;
    }

    static std::string sint(std::int64_t value) {
        return perform([=] (auto& iter, auto end) { return base128v::write_signed(value, iter, end); });
    }

    static std::string uint(std::uint64_t value) {
        return perform([=] (auto& iter, auto end) { return base128v::write_unsigned(value, iter, end); });
    }

    static std::string n_character(std::size_t n) {
        std::string results {};
        results.resize(n);
        for (std::size_t i = 0; i < n; ++i) {
            results[i] = static_cast<char>('A' + i % 26);
        }
        return results;
    }

    static std::string n_octet(std::size_t n) {
        std::string results {};
        results.resize(n);
        for (std::size_t i = 0; i < n; ++i) {
            results[i] = static_cast<char>(i);
        }
        return results;
    }

    static std::string n_bit(std::size_t n) {
        std::string results {};
        results.resize((n + 7) / 8);
        bitset bit { results.data(), n };
        for (std::size_t i = 0; i < n; ++i) {
            auto&& v = bit[i];
            if ((i % 2) == 0) {
                v.flip();
            }
            if ((i % 3) == 0) {
                v.flip();
            }
            if ((i % 7) == 0) {
                v.flip();
            }
        }
        return results;
    }

    template<class T, class U>
    static std::string fixed(U value) {
        static_assert(sizeof(T) == sizeof(U));
        T bits {};
        std::memcpy(&bits, &value, sizeof(T));

        std::string result {};
        result.resize(sizeof(bits));
        for (std::size_t i = 0; i < sizeof(bits); ++i) {
            result[i] = static_cast<char>(bits >> ((sizeof(bits) - i - 1U) * 8U));
        }
        return result;
    }
};

TEST_F(value_output_test, write_end_of_contents) {
    EXPECT_EQ(
            bytes({ header_end_of_contents }),
            perform([](auto& iter, auto end) { return write_end_of_contents(iter, end); }));
}

TEST_F(value_output_test, write_null) {
    EXPECT_EQ(
            bytes({ header_unknown }),
            perform([](auto& iter, auto end) { return write_null(iter, end); }));
}

TEST_F(value_output_test, write_int_embed_positive) {
    EXPECT_EQ(
            bytes({ header_embed_positive_int + 0 }),
            perform([](auto& iter, auto end) { return write_int(0, iter, end); }));
    EXPECT_EQ(
            bytes({ header_embed_positive_int + 63 }),
            perform([](auto& iter, auto end) { return write_int(63, iter, end); }));
}

TEST_F(value_output_test, write_int_embed_negative) {
    EXPECT_EQ(
            bytes({ header_embed_negative_int + 0 }),
            perform([](auto& iter, auto end) { return write_int(-16, iter, end); }));
    EXPECT_EQ(
            bytes({ header_embed_negative_int + 15 }),
            perform([](auto& iter, auto end) { return write_int(-1, iter, end); }));
}

TEST_F(value_output_test, write_int_full) {
    EXPECT_EQ(
            sequence(header_int, { sint(64) }),
            perform([](auto& iter, auto end) { return write_int(64, iter, end); }));
    EXPECT_EQ(
            sequence(header_int, { sint(-17) }),
            perform([](auto& iter, auto end) { return write_int(-17, iter, end); }));
    EXPECT_EQ(
            sequence(header_int, { sint(+1'000) }),
            perform([](auto& iter, auto end) { return write_int(+1'000, iter, end); }));
    EXPECT_EQ(
            sequence(header_int, { sint(-1'000) }),
            perform([](auto& iter, auto end) { return write_int(-1'000, iter, end); }));
    EXPECT_EQ(
            sequence(header_int, { sint(std::numeric_limits<std::int64_t>::max()) }),
            perform([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::max(), iter, end); }));
    EXPECT_EQ(
            sequence(header_int, { sint(std::numeric_limits<std::int64_t>::min()) }),
            perform([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::min(), iter, end); }));
}

TEST_F(value_output_test, write_float4) {
    EXPECT_EQ(
            sequence(header_float4, { bytes({ 0x3f, 0xa0, 0x00, 0x00 }) }),
            perform([](auto& iter, auto end) { return write_float4(1.25F, iter, end); }));
    EXPECT_EQ(
            sequence(header_float4, { fixed<std::uint32_t>(3.14F) }),
            perform([](auto& iter, auto end) { return write_float4(3.14F, iter, end); }));
}

TEST_F(value_output_test, write_float8) {
    EXPECT_EQ(
            sequence(header_float8, { fixed<std::uint64_t>(3.14) }),
            perform([](auto& iter, auto end) { return write_float8(3.14, iter, end); }));
}

TEST_F(value_output_test, write_decimal_int) {
    EXPECT_EQ(
            perform([](auto& iter, auto end) { return write_int(0, iter, end); }),
            perform([](auto& iter, auto end) { return write_decimal(0, iter, end); }));
    EXPECT_EQ(
            perform([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::max(), iter, end); }),
            perform([](auto& iter, auto end) { return write_decimal(std::numeric_limits<std::int64_t>::max(), iter, end); }));
    EXPECT_EQ(
            perform([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::min(), iter, end); }),
            perform([](auto& iter, auto end) { return write_decimal(std::numeric_limits<std::int64_t>::min(), iter, end); }));
}

TEST_F(value_output_test, write_decimal_compact) {
    EXPECT_EQ(
            sequence(header_decimal_compact, { sint(-2), sint(0) }),
            perform([](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { "0.00" }, iter, end); }));
    EXPECT_EQ(
            sequence(header_decimal_compact, { sint(-4), sint(31415) }),
            perform([](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { "3.1415" }, iter, end); }));
    EXPECT_EQ(
            sequence(header_decimal_compact, { sint(5), sint(std::numeric_limits<std::int64_t>::max()) }),
            perform([](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { std::numeric_limits<std::int64_t>::max(), 5 }, iter, end); }));
    EXPECT_EQ(
            sequence(header_decimal_compact, { sint(-5), sint(std::numeric_limits<std::int64_t>::min()) }),
            perform([](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { std::numeric_limits<std::int64_t>::min(), -5 }, iter, end); }));
}

TEST_F(value_output_test, write_decimal_full) {
    auto uimax = std::numeric_limits<std::uint64_t>::max();
    EXPECT_EQ(
            sequence(header_decimal, { sint(1), uint(9), bytes({ 0, 0x80, 0, 0, 0, 0, 0, 0, 0 }) }),
            perform([](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { +1, 0, 0x8000'0000'0000'0000ULL, 1 }, iter, end); }));
    EXPECT_EQ(
            sequence(header_decimal, { sint(-1), uint(9), bytes({ 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }) }),
            perform([](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { -1, 0, 0x8000'0000'0000'0001ULL, -1 }, iter, end); }));
    EXPECT_EQ(
            sequence(header_decimal, { sint(1), uint(17), bytes({
                    0,
                    0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff, }) }),
            perform([=](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { +1, uimax, uimax, 1, }, iter, end); }));
    EXPECT_EQ(
            sequence(header_decimal, { sint(-1), uint(17), bytes({
                    0xff,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x01, }) }),
            perform([=](auto& iter, auto end) { return write_decimal(takatori::decimal::triple { -1, uimax, uimax, -1, }, iter, end); }));
}

TEST_F(value_output_test, write_character_embed) {
    EXPECT_EQ(
            sequence(header_embed_character + 1 - 1, { "a" }),
            perform([](auto& iter, auto end) { return write_character("a", iter, end); }));
    EXPECT_EQ(
            sequence(header_embed_character + 64 - 1, { n_character(64) }),
            perform([](auto& iter, auto end) { return write_character(n_character(64), iter, end); }));
}

TEST_F(value_output_test, write_character_full) {
    EXPECT_EQ(
            sequence(header_character, { uint(0) }),
            perform([](auto& iter, auto end) { return write_character("", iter, end); }));
    EXPECT_EQ(
            sequence(header_character, { uint(65), n_character(65) }),
            perform([](auto& iter, auto end) { return write_character(n_character(65), iter, end); }));
    EXPECT_EQ(
            sequence(header_character, { uint(4096), n_character(4096) }),
            perform([](auto& iter, auto end) { return write_character(n_character(4096), iter, end); }, 4200));
}

TEST_F(value_output_test, write_octet_embed) {
    EXPECT_EQ(
            sequence(header_embed_octet + 1 - 1, { "a" }),
            perform([](auto& iter, auto end) { return write_octet("a", iter, end); }));
    EXPECT_EQ(
            sequence(header_embed_octet + 16 - 1, { n_octet(16) }),
            perform([](auto& iter, auto end) { return write_octet(n_octet(16), iter, end); }));
}

TEST_F(value_output_test, write_octet_full) {
    EXPECT_EQ(
            sequence(header_octet, { uint(0) }),
            perform([](auto& iter, auto end) { return write_octet("", iter, end); }));
    EXPECT_EQ(
            sequence(header_octet, { uint(17), n_octet(17) }),
            perform([](auto& iter, auto end) { return write_octet(n_octet(17), iter, end); }));
    EXPECT_EQ(
            sequence(header_octet, { uint(4096), n_octet(4096) }),
            perform([](auto& iter, auto end) { return write_octet(n_octet(4096), iter, end); }, 4200));
}

TEST_F(value_output_test, write_bit_embed) {
    EXPECT_EQ(
            sequence(header_embed_bit + 1 - 1, { bytes({ 0x01 }) }),
            perform([](auto& iter, auto end) { return write_bit(bytes({ 0xff }), 1, iter, end); }));
    EXPECT_EQ(
            sequence(header_embed_bit + 8 - 1, { n_bit(8) }),
            perform([](auto& iter, auto end) { return write_bit(n_bit(8), 8, iter, end); }));
}

TEST_F(value_output_test, write_bit_full) {
    EXPECT_EQ(
            sequence(header_bit, { uint(0) }),
            perform([](auto& iter, auto end) { return write_bit("", 0, iter, end); }));
    EXPECT_EQ(
            sequence(header_bit, { uint(17), n_bit(17) }),
            perform([](auto& iter, auto end) { return write_bit(n_bit(17), 17, iter, end); }));
    EXPECT_EQ(
            sequence(header_bit, { uint(4096), n_bit(4096) }),
            perform([](auto& iter, auto end) { return write_bit(n_bit(4096), 4096, iter, end); }, 520));
}

TEST_F(value_output_test, write_date) {
    EXPECT_EQ(
            sequence(header_date, { sint(0) }),
            perform([](auto& iter, auto end) { return write_date(takatori::datetime::date(0), iter, end); }));
    EXPECT_EQ(
            sequence(header_date, { sint(+1000) }),
            perform([](auto& iter, auto end) { return write_date(takatori::datetime::date(+1000), iter, end); }));
    EXPECT_EQ(
            sequence(header_date, { sint(-1000) }),
            perform([](auto& iter, auto end) { return write_date(takatori::datetime::date(-1000), iter, end); }));
}

TEST_F(value_output_test, write_time_of_day) {
    using unit = takatori::datetime::time_of_day::time_unit;
    EXPECT_EQ(
            sequence(header_time_of_day, { uint(0) }),
            perform([](auto& iter, auto end) { return write_time_of_day(takatori::datetime::time_of_day(unit(0)), iter, end); }));
    EXPECT_EQ(
            sequence(header_time_of_day, { uint(1000) }),
            perform([](auto& iter, auto end) { return write_time_of_day(takatori::datetime::time_of_day(unit(1000)), iter, end); }));
    EXPECT_EQ(
            sequence(header_time_of_day, { uint(86'400'000'000'000ULL - 1ULL) }),
            perform([](auto& iter, auto end) { return write_time_of_day(takatori::datetime::time_of_day(takatori::datetime::time_of_day::max_value), iter, end); }));
}

TEST_F(value_output_test, write_time_of_day_with_offset) {
    using unit = takatori::datetime::time_of_day::time_unit;
    EXPECT_EQ(
        sequence(header_time_of_day_with_offset, { uint(0), sint(0) }),
        perform([](auto& iter, auto end) { return write_time_of_day_with_offset(takatori::datetime::time_of_day(unit(0)), 0, iter, end); }));
    EXPECT_EQ(
        sequence(header_time_of_day_with_offset, { uint(1000), sint(15) }),
        perform([](auto& iter, auto end) { return write_time_of_day_with_offset(takatori::datetime::time_of_day(unit(1000)), 15, iter, end); }));
    EXPECT_EQ(
        sequence(header_time_of_day_with_offset, { uint(86'400'000'000'000ULL - 1ULL), sint(60*24) }),
        perform([](auto& iter, auto end) { return write_time_of_day_with_offset(takatori::datetime::time_of_day(takatori::datetime::time_of_day::max_value), 60*24, iter, end); }));
}

TEST_F(value_output_test, write_time_point) {
    using unit = takatori::datetime::time_point::offset_type;
    using ns = takatori::datetime::time_point::subsecond_unit;
    EXPECT_EQ(
            sequence(header_time_point, { sint(0), uint(0) }),
            perform([](auto& iter, auto end) { return write_time_point(takatori::datetime::time_point(), iter, end); }));
    EXPECT_EQ(
            sequence(header_time_point, { sint(1000), uint(0) }),
            perform([](auto& iter, auto end) { return write_time_point(takatori::datetime::time_point(unit(1000)), iter, end); }));
    EXPECT_EQ(
            sequence(header_time_point, { sint(-1000), uint(0) }),
            perform([](auto& iter, auto end) { return write_time_point(takatori::datetime::time_point(unit(-1000)), iter, end); }));
    EXPECT_EQ(
            sequence(header_time_point, { sint(0), uint(123456789) }),
            perform([](auto& iter, auto end) { return write_time_point(takatori::datetime::time_point({}, ns(123456789)), iter, end); }));
}

TEST_F(value_output_test, write_time_point_with_offset) {
    using unit = takatori::datetime::time_point::offset_type;
    using ns = takatori::datetime::time_point::subsecond_unit;
    EXPECT_EQ(
        sequence(header_time_point_with_offset, { sint(0), uint(0), sint(0) }),
        perform([](auto& iter, auto end) { return write_time_point_with_offset(takatori::datetime::time_point(), 0, iter, end); }));
    EXPECT_EQ(
        sequence(header_time_point_with_offset, { sint(1000), uint(0), sint(15) }),
        perform([](auto& iter, auto end) { return write_time_point_with_offset(takatori::datetime::time_point(unit(1000)), 15, iter, end); }));
    EXPECT_EQ(
        sequence(header_time_point_with_offset, { sint(-1000), uint(0), sint(-15) }),
        perform([](auto& iter, auto end) { return write_time_point_with_offset(takatori::datetime::time_point(unit(-1000)), -15, iter, end); }));
    EXPECT_EQ(
        sequence(header_time_point_with_offset, { sint(0), uint(123456789), sint(24*60) }),
        perform([](auto& iter, auto end) { return write_time_point_with_offset(takatori::datetime::time_point({}, ns(123456789)), 24*60, iter, end); }));
}

TEST_F(value_output_test, write_datetime_interval) {
    using ns = takatori::datetime::time_interval::time_unit;
    EXPECT_EQ(
            sequence(header_datetime_interval, { sint(0), sint(0), sint(0), sint(0) }),
            perform([](auto& iter, auto end) { return write_datetime_interval(takatori::datetime::datetime_interval(), iter, end); }));
    EXPECT_EQ(
            sequence(header_datetime_interval, { sint(1), sint(2), sint(3), sint(0) }),
            perform([](auto& iter, auto end) { return write_datetime_interval(takatori::datetime::date_interval(1, 2, 3), iter, end); }));
    EXPECT_EQ(
            sequence(header_datetime_interval, { sint(0), sint(0), sint(0), sint(100) }),
            perform([](auto& iter, auto end) { return write_datetime_interval(takatori::datetime::time_interval(ns(100)), iter, end); }));
}

TEST_F(value_output_test, write_array_begin_embed) {
    EXPECT_EQ(
            bytes({ header_embed_array + 1 - 1 }),
            perform([](auto& iter, auto end) { return write_array_begin(1, iter, end); }));
    EXPECT_EQ(
            bytes({ header_embed_array + 32 - 1 }),
            perform([](auto& iter, auto end) { return write_array_begin(32, iter, end); }));
}

TEST_F(value_output_test, write_array_begin_full) {
    EXPECT_EQ(
            sequence(header_array, { uint(0) }),
            perform([](auto& iter, auto end) { return write_array_begin(0, iter, end); }));
    EXPECT_EQ(
            sequence(header_array, { uint(33) }),
            perform([](auto& iter, auto end) { return write_array_begin(33, iter, end); }));
    EXPECT_EQ(
            sequence(header_array, { uint(4096) }),
            perform([](auto& iter, auto end) { return write_array_begin(4096, iter, end); }));
}

TEST_F(value_output_test, write_row_begin_embed) {
    EXPECT_EQ(
            bytes({ header_embed_row + 1 - 1 }),
            perform([](auto& iter, auto end) { return write_row_begin(1, iter, end); }));
    EXPECT_EQ(
            bytes({ header_embed_row + 32 - 1 }),
            perform([](auto& iter, auto end) { return write_row_begin(32, iter, end); }));
}

TEST_F(value_output_test, write_row_begin_full) {
    EXPECT_EQ(
            sequence(header_row, { uint(0) }),
            perform([](auto& iter, auto end) { return write_row_begin(0, iter, end); }));
    EXPECT_EQ(
            sequence(header_row, { uint(33) }),
            perform([](auto& iter, auto end) { return write_row_begin(33, iter, end); }));
    EXPECT_EQ(
            sequence(header_row, { uint(4096) }),
            perform([](auto& iter, auto end) { return write_row_begin(4096, iter, end); }));
}

TEST_F(value_output_test, DISABLED_write_clob) {
    FAIL() << "yet not implemented";
}

TEST_F(value_output_test, DISABLED_write_blob) {
    FAIL() << "yet not implemented";
}

} // namespace jogasaki::serializer
