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

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/primitive.h>
#include <takatori/type/simple_type.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/type/with_time_zone.h>
#include <yugawara/storage/column.h>
#include <yugawara/variable/nullity.h>
#include <tateyama/proto/kvs/data.pb.h>

#include <jogasaki/api/kvsservice/column_data.h>
#include <jogasaki/api/kvsservice/serializer.h>
#include <jogasaki/api/kvsservice/status.h>
#include <jogasaki/api/kvsservice/transaction_utils.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>

namespace jogasaki::api::kvsservice {

class serializer_test : public ::testing::Test {
public:
    void SetUp() override {
    }

    void TearDown() override {
    }

    void test(bool is_key, yugawara::storage::column const &column,
              tateyama::proto::kvs::data::Value &v1, tateyama::proto::kvs::data::Value &v2) {
        auto spec = is_key ? spec_primary_key : spec_value;
        std::vector<column_data> list{};
        list.emplace_back(&column, &v1);
        std::size_t size{};
        status s = get_bufsize(spec, list, size);
        ASSERT_EQ(s, status::ok);
        ASSERT_GT(size, 0);
        jogasaki::data::aligned_buffer buffer{size};
        jogasaki::kvs::writable_stream out_stream{buffer.data(), buffer.capacity()};
        s = serialize(spec, list, out_stream);
        ASSERT_EQ(s, status::ok);
        //
        jogasaki::kvs::readable_stream in_stream{out_stream.data(), out_stream.size()};
        s = deserialize(spec, column, in_stream, &v2);
        ASSERT_EQ(s, status::ok);
    }

};

TEST_F(serializer_test, ser_int4) {
    std::vector<std::int32_t> answers {0, 1, -1, 100, -1000,
                                       std::numeric_limits<int>::max(),
                                       std::numeric_limits<int>::min()};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::int4>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_int4_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.int4_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_int8) {
    std::vector<std::int64_t> answers {0, 1, -1, 100, -1000,
                                       std::numeric_limits<long>::max(),
                                       std::numeric_limits<long>::min()};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::int8>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_int8_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.int8_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_float4) {
    std::vector<float> answers {0, 1, -1, 100, -1000, 1.234e+10, -4.567e-10,
                                       std::numeric_limits<float>::max(),
                                       std::numeric_limits<float>::min()};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::float4>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_float4_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.float4_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_float8) {
    std::vector<double> answers {0, 1, -1, 100, -1000, 1.234e+10, -4.567e-10,
                                std::numeric_limits<double>::max(),
                                std::numeric_limits<double>::min()};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::float8>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_float8_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.float8_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_string) {
    std::vector<std::string> answers {"", "a", "ab", "abc", "abc\0def", "\0\1\2",
                                      "12345678901234567890"};
    std::size_t length = 256;
    for (auto answer : answers) {
        for (auto is_vary: {true, false}) {
            auto type{std::make_shared<takatori::type::character>(
                    takatori::type::varying_t(is_vary), length)};
            for (auto is_key: {true, false}) {
                yugawara::storage::column col{"col_name", type,
                                              {yugawara::variable::nullity(!is_key)},
                                              {}, {}};
                tateyama::proto::kvs::data::Value v1{};
                tateyama::proto::kvs::data::Value v2{};
                v1.set_character_value(answer);
                test(is_key, col, v1, v2);
                EXPECT_EQ(v2.character_value(), answer);
            }
        }
    }
}

TEST_F(serializer_test, ser_bool) {
    std::vector<bool> answers {true, false};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::boolean>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_boolean_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.boolean_value(), answer);
        }
    }
}

// NOTE see set_decimal() in src/jogasaki/api/kvsservice/serializer.cpp
static tateyama::proto::kvs::data::Decimal dec(const std::uint64_t hi, const std::uint64_t lo, const int exp) noexcept {
    std::string buf{};
    auto bufsize = sizeof(lo) + sizeof(hi);
    buf.reserve(bufsize);
    auto v = lo;
    for (int i = 0; i < 8; i++) {
        buf[15 - i] = static_cast<std::uint8_t>(v & 0xffU);
        v >>= 8U;
    }
    v = hi;
    for (int i = 0; i < 8; i++) {
        buf[7 - i] = static_cast<std::uint8_t>(v & 0xffU);
        v >>= 8U;
    }
    // skip zero-padding
    std::size_t start = 0;
    while (start < bufsize - 1) {
        if (buf[start] != 0) {
            break;
        }
        start++;
    }
    tateyama::proto::kvs::data::Decimal d{};
    // NOTE buf.size() returns 0, not 16
    d.set_unscaled_value(std::addressof(buf[start]), bufsize - start);
    d.set_exponent(exp);
    return d;
}

TEST_F(serializer_test, ser_decimal) {
    int precision = 5;
    int exp = -3;
    auto type = std::make_shared<takatori::type::decimal>(precision, -exp);
    for (auto is_key: {true, false}) {
        yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
        tateyama::proto::kvs::data::Value v1{};
        // test by 12.345
        auto answer = dec(0, 12345, exp);
        tateyama::proto::kvs::data::Value v2{};
        v1.set_allocated_decimal_value(&answer);
        test(is_key, col, v1, v2);
        EXPECT_EQ(v2.decimal_value().unscaled_value(), answer.unscaled_value());
        EXPECT_EQ(v2.decimal_value().exponent(), answer.exponent());
        v1.release_decimal_value();
    }
}

TEST_F(serializer_test, ser_date) {
    // date (number of days offset of epoch 1970-01-01).
    // sint64 date_value = 15;

    // NOTE
    // third_party/mizugaki/third_party/yugawara/third_party/takatori/src/takatori/datetime/date_util.h
    // @brief the max year.
    // static constexpr difference_type max_days = +365'241'780'471LL;
    // @brief the min year.
    // static constexpr difference_type min_days = -365'243'219'162LL;
    std::vector<std::int64_t> answers {0, 1, -1, 100, -1000,
                                       std::numeric_limits<int>::max(),
                                       std::numeric_limits<int>::min()};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::simple_type<takatori::type::type_kind::date>>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_date_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.date_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_time_of_day) {
    // time of day (nano-seconds since 00:00:00).
    // uint64 time_of_day_value = 16;
    std::vector<std::uint64_t> answers {0, 100, 10000, 3 * 3600UL * 1'000'000'000UL};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::simple_type<takatori::type::type_kind::time_of_day>>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_time_of_day_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.time_of_day_value(), answer);
        }
    }
}

TEST_F(serializer_test, DISABLED_ser_time_of_day_timezone) {
    // offset nano-seconds from epoch (00:00:00) in the time zone.
    // uint64 offset_nanoseconds = 1;
    // timezone offset in minute.
    // sint32 time_zone_offset = 2;
    std::vector<std::uint64_t> nanosecs {0, 100, 10000, 3 * 3600UL * 1'000'000'000UL};
    std::vector<int> offsets {0, 1, -1, 60, -60, 12 * 60, -12 * 60};
    for (auto nanosec : nanosecs) {
        for (auto offset : offsets) {
            takatori::type::with_time_zone_t with{true};
            auto type = std::make_shared<takatori::type::time_of_day>(with);
            for (auto is_key: {true, false}) {
                yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
                tateyama::proto::kvs::data::Value v1{};
                tateyama::proto::kvs::data::Value v2{};
                tateyama::proto::kvs::data::TimeOfDayWithTimeZone td{};
                td.set_offset_nanoseconds(nanosec);
                td.set_time_zone_offset(offset);
                v1.set_allocated_time_of_day_with_time_zone_value(&td);
                test(is_key, col, v1, v2);
                EXPECT_EQ(v2.time_of_day_with_time_zone_value().offset_nanoseconds(), nanosec);
                EXPECT_EQ(v2.time_of_day_with_time_zone_value().time_zone_offset(), offset);
                v1.release_time_of_day_with_time_zone_value();
            }
        }
    }
}

static tateyama::proto::kvs::data::TimePoint timepoint(const long sec, const uint32_t nano) noexcept {
    tateyama::proto::kvs::data::TimePoint tp{};
    tp.set_offset_seconds(sec);
    tp.set_nano_adjustment(nano);
    return tp;
}

TEST_F(serializer_test, ser_timepoint) {
    // offset seconds from epoch (1970-01-01 00:00:00).
    // sint64 offset_seconds = 1;
    // nano-seconds adjustment [0, 10^9-1].
    // uint32 nano_adjustment = 2;
    std::vector<tateyama::proto::kvs::data::TimePoint> answers {
        timepoint(0, 0), timepoint(1234, 567)};
    for (auto &answer : answers) {
        auto type { std::make_shared<takatori::type::simple_type<takatori::type::type_kind::time_point>>() };
        for (auto is_key: {true, false}) {
            yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_allocated_time_point_value(&answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.time_point_value().offset_seconds(), answer.offset_seconds());
            EXPECT_EQ(v2.time_point_value().nano_adjustment(), answer.nano_adjustment());
            v1.release_time_point_value();
        }
    }
}

TEST_F(serializer_test, DISABLED_ser_timepoint_timezone) {
    // offset seconds from epoch (1970-01-01 00:00:00) in the time zone.
    // sint64 offset_seconds = 1;
    // nano-seconds adjustment [0, 10^9-1].
    // uint32 nano_adjustment = 2;
    // timezone offset in minute.
    // sint32 time_zone_offset = 3;
    std::vector<tateyama::proto::kvs::data::TimePoint> answers {
            timepoint(0, 0), timepoint(1234, 567)};
    std::vector<int> offsets {0, 1, -1, 60, -60, 12 * 60, -12 * 60};
    for (auto answer : answers) {
        for (auto offset : offsets) {
            for (auto has_tz: {true, false}) {
                takatori::type::with_time_zone_t with{has_tz};
                auto type = std::make_shared<takatori::type::time_point>(with);
                for (auto is_key: {true, false}) {
                    yugawara::storage::column col{"col_name", type, {yugawara::variable::nullity(!is_key)}, {}, {}};
                    tateyama::proto::kvs::data::Value v1{};
                    tateyama::proto::kvs::data::Value v2{};
                    tateyama::proto::kvs::data::TimePointWithTimeZone tpz1{};
                    tpz1.set_offset_seconds(answer.offset_seconds());
                    tpz1.set_nano_adjustment(answer.nano_adjustment());
                    tpz1.set_time_zone_offset(offset);
                    v1.set_allocated_time_point_with_time_zone_value(&tpz1);
                    test(is_key, col, v1, v2);
                    const auto &tpz2 = v2.time_point_with_time_zone_value();
                    EXPECT_EQ(tpz2.offset_seconds(), answer.offset_seconds());
                    EXPECT_EQ(tpz2.nano_adjustment(), answer.nano_adjustment());
                    EXPECT_EQ(tpz2.time_zone_offset(), offset);
                    v1.release_time_point_with_time_zone_value();
                }
            }
        }
    }
}
}
