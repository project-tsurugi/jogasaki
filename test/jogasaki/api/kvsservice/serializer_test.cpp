/*
 * Copyright 2018-2023 tsurugi project.
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

#include <gtest/gtest.h>

#include <tateyama/proto/kvs/data.pb.h>
#include <takatori/type/decimal.h>
#include <takatori/type/primitive.h>
#include <jogasaki/api/kvsservice/serializer.h>
#include <jogasaki/api/kvsservice/transaction_utils.h>

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
        auto nullable = is_key ? nullable_primary_key : nullable_value;
        std::vector<column_data> list{};
        list.emplace_back(&column, &v1);
        auto size = get_bufsize(spec, nullable, list);
        ASSERT_GT(size, 0);
        jogasaki::data::aligned_buffer buffer{size};
        jogasaki::kvs::writable_stream out_stream{buffer.data(), buffer.capacity()};
        auto s = serialize(spec, nullable, list, out_stream);
        ASSERT_EQ(s, status::ok);
        //
        jogasaki::kvs::readable_stream in_stream{out_stream.data(), out_stream.size()};
        s = deserialize(spec, nullable, column, in_stream, &v2);
        ASSERT_EQ(s, status::ok);
    }

};

TEST_F(serializer_test, ser_int4) {
    std::vector<std::int32_t> answers {0, 1, -1, 100, -1000,
                                       std::numeric_limits<int>::max(),
                                       std::numeric_limits<int>::min()};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::int4>() };
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
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
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
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
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
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
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
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
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::simple_type<takatori::type::type_kind::character>>() };
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_character_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.character_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_bool) {
    std::vector<bool> answers {true, false};
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::boolean>() };
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_boolean_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.boolean_value(), answer);
        }
    }
}

static tateyama::proto::kvs::data::Decimal dec(const std::uint64_t hi, const std::uint64_t lo, const int exp) noexcept {
    std::string buf{};
    buf.reserve(sizeof(lo) + sizeof(hi));
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
    tateyama::proto::kvs::data::Decimal d{};
    d.set_unscaled_value(buf.data(), buf.size());
    d.set_exponent(exp);
    return d;
}

TEST_F(serializer_test, ser_decimal) {
    std::vector<tateyama::proto::kvs::data::Decimal> answers {
        dec(0, 0, 0), dec(0, 1, 0), dec(1, 0, 0), dec(0, 123456, -3)};
    int precision = 5;
    for (auto answer : answers) {
        auto type = std::make_shared<takatori::type::decimal>(precision, -answer.exponent());
        yugawara::storage::column col{"col_name", std::move(type), {}, {}, {}};
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            auto *d = new tateyama::proto::kvs::data::Decimal;
            d->set_unscaled_value(answer.unscaled_value());
            d->set_exponent(answer.exponent());
            v1.set_allocated_decimal_value(d);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.decimal_value().unscaled_value(), answer.unscaled_value());
            EXPECT_EQ(v2.decimal_value().exponent(), answer.exponent());
        }
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
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
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
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_time_of_day_value(answer);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.time_of_day_value(), answer);
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
    for (auto answer : answers) {
        auto type { std::make_shared<takatori::type::simple_type<takatori::type::type_kind::time_point>>() };
        yugawara::storage::column col{"col_name", type, {}, {}, {}};
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            auto *tp = new tateyama::proto::kvs::data::TimePoint;
            tp->set_offset_seconds(answer.offset_seconds());
            tp->set_nano_adjustment(answer.nano_adjustment());
            v1.set_allocated_time_point_value(tp);
            test(is_key, col, v1, v2);
            EXPECT_EQ(v2.time_point_value().offset_seconds(), answer.offset_seconds());
            EXPECT_EQ(v2.time_point_value().nano_adjustment(), answer.nano_adjustment());
        }
    }
}

}
