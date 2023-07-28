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
#include <jogasaki/api/kvsservice/serializer.h>
#include <jogasaki/api/kvsservice/transaction_utils.h>

namespace jogasaki::api::kvsservice {

class serializer_test : public ::testing::Test {
public:
    void SetUp() override {
    }

    void TearDown() override {
    }

    void test(bool is_key, takatori::type::type_kind const kind,
              tateyama::proto::kvs::data::Value &v1, tateyama::proto::kvs::data::Value &v2) {
        auto spec = is_key ? spec_primary_key : spec_value;
        auto nullable = is_key ? nullable_primary_key : nullable_value;
        std::vector<tateyama::proto::kvs::data::Value const *> values{&v1};
        auto size = get_bufsize(spec, nullable, values);
        ASSERT_GT(size, 0);
        jogasaki::data::aligned_buffer buffer{size};
        jogasaki::kvs::writable_stream out_stream{buffer.data(), buffer.capacity()};
        auto s = serialize(spec, nullable, values, out_stream);
        ASSERT_EQ(s, status::ok);
        //
        jogasaki::kvs::readable_stream in_stream{out_stream.data(), out_stream.size()};
        s = deserialize(spec, nullable, kind, in_stream, &v2);
        ASSERT_EQ(s, status::ok);
    }
};

TEST_F(serializer_test, ser_int4) {
    std::vector<std::int32_t> answers {0, 1, -1, 100, -1000,
                                       std::numeric_limits<int>::max(),
                                       std::numeric_limits<int>::min()};
    for (auto answer : answers) {
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_int4_value(answer);
            test(is_key, takatori::type::type_kind::int4, v1, v2);
            EXPECT_EQ(v2.int4_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_int8) {
    std::vector<std::int64_t> answers {0, 1, -1, 100, -1000,
                                       std::numeric_limits<long>::max(),
                                       std::numeric_limits<long>::min()};
    for (auto answer : answers) {
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_int8_value(answer);
            test(is_key, takatori::type::type_kind::int8, v1, v2);
            EXPECT_EQ(v2.int8_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_float4) {
    std::vector<float> answers {0, 1, -1, 100, -1000, 1.234e+10, -4.567e-10,
                                       std::numeric_limits<float>::max(),
                                       std::numeric_limits<float>::min()};
    for (auto answer : answers) {
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_float4_value(answer);
            test(is_key, takatori::type::type_kind::float4, v1, v2);
            EXPECT_EQ(v2.float4_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_float8) {
    std::vector<double> answers {0, 1, -1, 100, -1000, 1.234e+10, -4.567e-10,
                                std::numeric_limits<double>::max(),
                                std::numeric_limits<double>::min()};
    for (auto answer : answers) {
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_float8_value(answer);
            test(is_key, takatori::type::type_kind::float8, v1, v2);
            EXPECT_EQ(v2.float8_value(), answer);
        }
    }
}

TEST_F(serializer_test, ser_string) {
    std::vector<std::string> answers {"", "a", "ab", "abc", "abc\0def", "\0\1\2",
                                      "12345678901234567890"};
    for (auto answer : answers) {
        for (auto is_key: {true, false}) {
            tateyama::proto::kvs::data::Value v1{};
            tateyama::proto::kvs::data::Value v2{};
            v1.set_character_value(answer);
            test(is_key, takatori::type::type_kind::character, v1, v2);
            EXPECT_EQ(v2.character_value(), answer);
        }
    }
}
}
