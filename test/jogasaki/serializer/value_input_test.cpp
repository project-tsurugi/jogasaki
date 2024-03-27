#include <cstring>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <gtest/gtest.h>

#include <takatori/datetime/time_interval.h>
#include <takatori/util/basic_bitset_view.h>
#include <takatori/util/basic_buffer_view.h>

#include <jogasaki/serializer/value_input.h>
#include <jogasaki/serializer/value_output.h>


namespace jogasaki::serializer {

using buffer = takatori::util::buffer_view;
using cbuffer = takatori::util::const_buffer_view;
using bitset = takatori::util::bitset_view;
using cbitset = takatori::util::const_bitset_view;

class value_input_test : public ::testing::Test {
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

    static std::string dump(
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

    template<class T = void>
    static T restore(
            std::string const& input,
            std::function<T(buffer::const_iterator&, buffer::const_iterator)> const& action) {
        cbuffer view { input.data(), input.size() };
        auto const* iter = view.begin();
        if constexpr (std::is_same_v<T, void>) {
            action(iter, view.end());
            if (iter != view.end()) {
                throw std::runtime_error("extra bytes");
            }
        } else {
            auto result = action(iter, view.end());
            if (iter != view.end()) {
                throw std::runtime_error("extra bytes");
            }
            return result;
        }
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

    bitset n_bit_view(std::size_t n) {
        last_ = n_bit(n);
        return bitset { last_.data(), n };
    }

private:
    std::string last_;
};

TEST_F(value_input_test, read_end_of_contents) {
    auto buf = dump([](auto& iter, auto end) { return write_end_of_contents(iter, end); });
    restore<void>(buf, [](auto& iter, auto end) { read_end_of_contents(iter, end); });
}

TEST_F(value_input_test, read_null) {
    auto buf = dump([](auto& iter, auto end) { return write_null(iter, end); });
    restore<void>(buf, [](auto& iter, auto end) { read_null(iter, end); });
}

TEST_F(value_input_test, read_int_embed_positive) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(0, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, 0);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(63, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, 63);
    }
}

TEST_F(value_input_test, read_int_embed_negative) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(-16, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, -16);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(-1, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, -1);
    }
}

TEST_F(value_input_test, read_int_full) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(64, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, 64);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(-17, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, -17);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(+1'000, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, +1'000);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(-1'000, iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, -1'000);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::max(), iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, std::numeric_limits<std::int64_t>::max());
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::min(), iter, end); });
        auto result = restore<std::int64_t>(buf, [](auto& iter, auto end) { return read_int(iter, end); });
        EXPECT_EQ(result, std::numeric_limits<std::int64_t>::min());
    }
}

TEST_F(value_input_test, read_float4) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_float4(1.25F, iter, end); });
        auto result = restore<float>(buf, [](auto& iter, auto end) { return read_float4(iter, end); });
        EXPECT_EQ(result, 1.25F);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_float4(3.14F, iter, end); });
        auto result = restore<float>(buf, [](auto& iter, auto end) { return read_float4(iter, end); });
        EXPECT_EQ(result, 3.14F);
    }
}

TEST_F(value_input_test, read_float8) {
    auto buf = dump([](auto& iter, auto end) { return write_float8(3.14, iter, end); });
    auto result = restore<double>(buf, [](auto& iter, auto end) { return read_float8(iter, end); });
    EXPECT_EQ(result, 3.14);
}

TEST_F(value_input_test, read_decimal_int) {
    using decimal = takatori::decimal::triple;
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(0, iter, end); });
        auto result = restore<decimal>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, 0);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::max(), iter, end); });
        auto result = restore<decimal>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, std::numeric_limits<std::int64_t>::max());
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_int(std::numeric_limits<std::int64_t>::min(), iter, end); });
        auto result = restore<decimal>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, std::numeric_limits<std::int64_t>::min());
    }
}

TEST_F(value_input_test, read_decimal_compact) {
    {
        takatori::decimal::triple value { 0, -2 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
    {
        takatori::decimal::triple value { 31415, -4 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
    {
        takatori::decimal::triple value { std::numeric_limits<std::int64_t>::max(), 5 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
    {
        takatori::decimal::triple value { std::numeric_limits<std::int64_t>::min(), -5 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
}

TEST_F(value_input_test, read_decimal_full) {
    {
        takatori::decimal::triple value { +1, 0, 0x8000'0000'0000'0000ULL, 1 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
    {
        takatori::decimal::triple value { -1, 0, 0x8000'0000'0000'0001ULL, -1 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
    {
        takatori::decimal::triple value { +1, std::numeric_limits<std::uint64_t>::max(), std::numeric_limits<std::uint64_t>::max(), +1 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
    {
        takatori::decimal::triple value { -1, std::numeric_limits<std::uint64_t>::max(), std::numeric_limits<std::uint64_t>::max(), -1 };
        auto buf = dump([=](auto& iter, auto end) { return write_decimal(value, iter, end); });
        auto result = restore<takatori::decimal::triple>(buf, [](auto& iter, auto end) { return read_decimal(iter, end); });
        EXPECT_EQ(result, value);
    }
}

TEST_F(value_input_test, read_character_embed) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_character("a", iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_character(iter, end); });
        EXPECT_EQ(result, "a");
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_character(n_character(64), iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_character(iter, end); });
        EXPECT_EQ(result, n_character(64));
    }
}

TEST_F(value_input_test, read_character_full) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_character("", iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_character(iter, end); });
        EXPECT_EQ(result, "");
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_character(n_character(65), iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_character(iter, end); });
        EXPECT_EQ(result, n_character(65));
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_character(n_character(4096), iter, end); }, 4200);
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_character(iter, end); });
        EXPECT_EQ(result, n_character(4096));
    }
}

TEST_F(value_input_test, read_octet_embed) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_octet("a", iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_octet(iter, end); });
        EXPECT_EQ(result, "a");
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_octet(n_octet(16), iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_octet(iter, end); });
        EXPECT_EQ(result, n_octet(16));
    }
}

TEST_F(value_input_test, read_octet_full) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_octet("", iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_octet(iter, end); });
        EXPECT_EQ(result, "");
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_octet(n_octet(17), iter, end); });
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_octet(iter, end); });
        EXPECT_EQ(result, n_octet(17));
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_octet(n_octet(4096), iter, end); }, 4200);
        auto result = restore<std::string_view>(buf, [](auto& iter, auto end) { return read_octet(iter, end); });
        EXPECT_EQ(result, n_octet(4096));
    }
}

TEST_F(value_input_test, read_bit_embed) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_bit(n_bit(1), 1, iter, end); });
        auto result = restore<cbitset>(buf, [](auto& iter, auto end) { return read_bit(iter, end); });
        EXPECT_EQ(result, n_bit_view(1));
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_bit(n_bit(8), 8, iter, end); });
        auto result = restore<cbitset>(buf, [](auto& iter, auto end) { return read_bit(iter, end); });
        EXPECT_EQ(result, n_bit_view(8));
    }
}

TEST_F(value_input_test, read_bit_full) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_bit("", 0, iter, end); });
        auto result = restore<cbitset>(buf, [](auto& iter, auto end) { return read_bit(iter, end); });
        EXPECT_TRUE(result.empty());
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_bit(n_bit(17), 17, iter, end); });
        auto result = restore<cbitset>(buf, [](auto& iter, auto end) { return read_bit(iter, end); });
        EXPECT_EQ(result, n_bit_view(17));
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_bit(n_bit(4096), 4096, iter, end); }, 520);
        auto result = restore<cbitset>(buf, [](auto& iter, auto end) { return read_bit(iter, end); });
        EXPECT_EQ(result, n_bit_view(4096));
    }
}

TEST_F(value_input_test, write_date) {
    {
        takatori::datetime::date input;
        auto buf = dump([=](auto& iter, auto end) { return write_date(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_date(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::date input { +1000 };
        auto buf = dump([=](auto& iter, auto end) { return write_date(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_date(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::date input { -1000 };
        auto buf = dump([=](auto& iter, auto end) { return write_date(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_date(iter, end); });
        EXPECT_EQ(result, input);
    }
}

TEST_F(value_input_test, write_time_of_day) {
    {
        takatori::datetime::time_of_day input;
        auto buf = dump([=](auto& iter, auto end) { return write_time_of_day(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_of_day(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::time_of_day input {
                takatori::datetime::time_of_day::time_unit { 1000 },
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_of_day(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_of_day(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::time_of_day input { takatori::datetime::time_of_day::max_value };
        auto buf = dump([=](auto& iter, auto end) { return write_time_of_day(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_of_day(iter, end); });
        EXPECT_EQ(result, input);
    }
}

TEST_F(value_input_test, write_time_of_day_with_offset) {
    {
        std::pair<takatori::datetime::time_of_day, std::int32_t> input;
        auto buf = dump([=](auto& iter, auto end) { return write_time_of_day_with_offset(input.first, input.second, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_of_day_with_offset(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        std::pair<takatori::datetime::time_of_day, std::int32_t> input{
            takatori::datetime::time_of_day {
                takatori::datetime::time_of_day::time_unit { 1000 },
            },
            15
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_of_day_with_offset(input.first, input.second, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_of_day_with_offset(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        std::pair<takatori::datetime::time_of_day, std::int32_t> input {
            takatori::datetime::time_of_day::max_value,
            24*60
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_of_day_with_offset(input.first, input.second, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_of_day_with_offset(iter, end); });
        EXPECT_EQ(result, input);
    }
}

TEST_F(value_input_test, write_time_point) {
    {
        takatori::datetime::time_point input {};
        auto buf = dump([=](auto& iter, auto end) { return write_time_point(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::time_point input {
                takatori::datetime::time_point::offset_type { 1000 },
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_point(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::time_point input {
                takatori::datetime::time_point::offset_type { -1000 },
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_point(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::time_point input {
            takatori::datetime::time_point::offset_type { 1000 },
            takatori::datetime::time_point::subsecond_unit { 123456789 },
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_point(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point(iter, end); });
        EXPECT_EQ(result, input);
    }
}

TEST_F(value_input_test, write_time_point_with_offset) {
    {
        std::pair<takatori::datetime::time_point, std::int32_t> input {};
        auto buf = dump([=](auto& iter, auto end) { return write_time_point_with_offset(input.first, input.second, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point_with_offset(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        std::pair<takatori::datetime::time_point, std::int32_t> input {
            takatori::datetime::time_point {
                takatori::datetime::time_point::offset_type { 1000 },
            },
            15
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_point_with_offset(input.first, input.second, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point_with_offset(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        std::pair<takatori::datetime::time_point, std::int32_t> input {
            takatori::datetime::time_point {
                takatori::datetime::time_point::offset_type { -1000 },
            },
            -15
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_point_with_offset(input.first, input.second, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point_with_offset(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        std::pair<takatori::datetime::time_point, std::int32_t> input {
            takatori::datetime::time_point {
                takatori::datetime::time_point::offset_type { 1000 },
                    takatori::datetime::time_point::subsecond_unit { 123456789 },
            },
            -24*60
        };
        auto buf = dump([=](auto& iter, auto end) { return write_time_point_with_offset(input.first, input.second, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_time_point_with_offset(iter, end); });
        EXPECT_EQ(result, input);
    }
}

TEST_F(value_input_test, write_datetime_interval) {
    {
        takatori::datetime::datetime_interval input {};
        auto buf = dump([=](auto& iter, auto end) { return write_datetime_interval(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_datetime_interval(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::datetime_interval input {
                { 1, 2, 3 },
                {},
        };
        auto buf = dump([=](auto& iter, auto end) { return write_datetime_interval(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_datetime_interval(iter, end); });
        EXPECT_EQ(result, input);
    }
    {
        takatori::datetime::datetime_interval input {
                {},
                takatori::datetime::time_interval {
                        takatori::datetime::time_interval::time_unit { 100 }
                }
        };
        auto buf = dump([=](auto& iter, auto end) { return write_datetime_interval(input, iter, end); });
        auto result = restore<decltype(input)>(buf, [](auto& iter, auto end) { return read_datetime_interval(iter, end); });
        EXPECT_EQ(result, input);
    }
}

TEST_F(value_input_test, read_array_begin_embed) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_array_begin(1, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_array_begin(iter, end); });
        EXPECT_EQ(result, 1);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_array_begin(32, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_array_begin(iter, end); });
        EXPECT_EQ(result, 32);
    }
}

TEST_F(value_input_test, read_array_begin_full) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_array_begin(0, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_array_begin(iter, end); });
        EXPECT_EQ(result, 0);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_array_begin(33, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_array_begin(iter, end); });
        EXPECT_EQ(result, 33);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_array_begin(4096, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_array_begin(iter, end); });
        EXPECT_EQ(result, 4096);
    }
}

TEST_F(value_input_test, read_row_begin_embed) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_row_begin(1, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_row_begin(iter, end); });
        EXPECT_EQ(result, 1);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_row_begin(32, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_row_begin(iter, end); });
        EXPECT_EQ(result, 32);
    }
}

TEST_F(value_input_test, read_row_begin_full) {
    {
        auto buf = dump([](auto& iter, auto end) { return write_row_begin(0, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_row_begin(iter, end); });
        EXPECT_EQ(result, 0);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_row_begin(33, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_row_begin(iter, end); });
        EXPECT_EQ(result, 33);
    }
    {
        auto buf = dump([](auto& iter, auto end) { return write_row_begin(4096, iter, end); });
        auto result = restore<std::uint64_t>(buf, [](auto& iter, auto end) { return read_row_begin(iter, end); });
        EXPECT_EQ(result, 4096);
    }
}

TEST_F(value_input_test, DISABLED_write_clob) {
    FAIL() << "yet not implemented";
}

TEST_F(value_input_test, DISABLED_write_blob) {
    FAIL() << "yet not implemented";
}

} // namespace jogasaki::serializer
