/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/data/any_sequence_stream_status.h>
#include <jogasaki/data/mock_any_sequence_stream.h>
#include <jogasaki/test_root.h>

namespace jogasaki::data {

using namespace testing;

// helper function to create any from int64_t
inline any make_any_int64(std::int64_t v) {
    return any{std::in_place_type<std::int64_t>, v};
}

class any_sequence_test : public test_root {
public:
};

TEST_F(any_sequence_test, default_constructor) {
    any_sequence seq{};
    EXPECT_TRUE(seq.empty());
    EXPECT_EQ(0, seq.size());
}

TEST_F(any_sequence_test, initializer_list_constructor) {
    any_sequence seq{make_any_int64(100), make_any_int64(200), make_any_int64(300)};
    EXPECT_TRUE(! seq.empty());
    EXPECT_EQ(3, seq.size());
    EXPECT_EQ(100, seq[0].to<std::int64_t>());
    EXPECT_EQ(200, seq[1].to<std::int64_t>());
    EXPECT_EQ(300, seq[2].to<std::int64_t>());
}

TEST_F(any_sequence_test, vector_constructor) {
    std::vector<any> values{};
    values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{1});
    values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{2});
    any_sequence seq{std::move(values)};
    EXPECT_EQ(2, seq.size());
    EXPECT_EQ(1, seq[0].to<std::int64_t>());
    EXPECT_EQ(2, seq[1].to<std::int64_t>());
}

TEST_F(any_sequence_test, view_constructor) {
    std::vector<any> values{};
    values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{10});
    values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{20});
    any_sequence::view_type view{values};
    any_sequence seq{view};
    EXPECT_EQ(2, seq.size());
    EXPECT_EQ(10, seq[0].to<std::int64_t>());
    EXPECT_EQ(20, seq[1].to<std::int64_t>());
}

TEST_F(any_sequence_test, iteration) {
    any_sequence seq{make_any_int64(5), make_any_int64(6), make_any_int64(7)};
    std::vector<std::int64_t> collected{};
    for (auto const& val : seq) {
        collected.push_back(val.to<std::int64_t>());
    }
    EXPECT_EQ(3, collected.size());
    EXPECT_EQ(5, collected[0]);
    EXPECT_EQ(6, collected[1]);
    EXPECT_EQ(7, collected[2]);
}

TEST_F(any_sequence_test, clear) {
    any_sequence seq{make_any_int64(1), make_any_int64(2)};
    EXPECT_TRUE(! seq.empty());
    seq.clear();
    EXPECT_TRUE(seq.empty());
    EXPECT_EQ(0, seq.size());
}

TEST_F(any_sequence_test, assign_storage) {
    any_sequence seq{};
    EXPECT_TRUE(seq.empty());

    std::vector<any> values{};
    values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{100});
    seq.assign(std::move(values));
    EXPECT_EQ(1, seq.size());
    EXPECT_EQ(100, seq[0].to<std::int64_t>());
}

TEST_F(any_sequence_test, assign_view) {
    any_sequence seq{make_any_int64(1)};
    EXPECT_EQ(1, seq.size());

    std::vector<any> new_values{};
    new_values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{10});
    new_values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{20});
    new_values.emplace_back(std::in_place_type<std::int64_t>, std::int64_t{30});
    any_sequence::view_type view{new_values};
    seq.assign(view);
    EXPECT_EQ(3, seq.size());
    EXPECT_EQ(10, seq[0].to<std::int64_t>());
}

TEST_F(any_sequence_test, equality) {
    any_sequence seq1{make_any_int64(1), make_any_int64(2)};
    any_sequence seq2{make_any_int64(1), make_any_int64(2)};
    any_sequence seq3{make_any_int64(1), make_any_int64(3)};
    any_sequence seq4{make_any_int64(1)};

    EXPECT_TRUE(seq1 == seq2);
    EXPECT_TRUE(! (seq1 == seq3));
    EXPECT_TRUE(! (seq1 == seq4));
    EXPECT_TRUE(seq1 != seq3);
}

TEST_F(any_sequence_test, output_stream) {
    any_sequence seq{make_any_int64(42), make_any_int64(43)};
    std::stringstream ss{};
    ss << seq;
    std::string result = ss.str();
    // just verify it doesn't crash and produces some output
    EXPECT_TRUE(! result.empty());
}

// mock_any_sequence_stream tests

TEST_F(any_sequence_test, builtin_stream_empty) {
    mock_any_sequence_stream stream{};
    any_sequence seq{};
    auto status = stream.next(seq, std::nullopt);
    EXPECT_EQ(any_sequence_stream_status::end_of_stream, status);
}

TEST_F(any_sequence_test, builtin_stream_single_row) {
    mock_any_sequence_stream stream{
        any_sequence{make_any_int64(100), make_any_int64(200)}
    };

    any_sequence seq{};
    auto status = stream.next(seq, std::nullopt);
    EXPECT_EQ(any_sequence_stream_status::ok, status);
    EXPECT_EQ(2, seq.size());
    EXPECT_EQ(100, seq[0].to<std::int64_t>());
    EXPECT_EQ(200, seq[1].to<std::int64_t>());

    status = stream.next(seq, std::nullopt);
    EXPECT_EQ(any_sequence_stream_status::end_of_stream, status);
}

TEST_F(any_sequence_test, builtin_stream_multiple_rows) {
    mock_any_sequence_stream stream{
        any_sequence{make_any_int64(1)},
        any_sequence{make_any_int64(2)},
        any_sequence{make_any_int64(3)}
    };

    any_sequence seq{};
    std::vector<std::int64_t> values{};

    while (stream.next(seq, std::nullopt) == any_sequence_stream_status::ok) {
        values.push_back(seq[0].to<std::int64_t>());
    }

    EXPECT_EQ(3, values.size());
    EXPECT_EQ(1, values[0]);
    EXPECT_EQ(2, values[1]);
    EXPECT_EQ(3, values[2]);
}

TEST_F(any_sequence_test, builtin_stream_try_next) {
    mock_any_sequence_stream stream{
        any_sequence{make_any_int64(42)}
    };

    any_sequence seq{};

    // try_next should succeed for available data
    auto status = stream.try_next(seq);
    EXPECT_EQ(any_sequence_stream_status::ok, status);
    EXPECT_EQ(42, seq[0].to<std::int64_t>());

    // after consuming all data, try_next returns end_of_stream
    status = stream.try_next(seq);
    EXPECT_EQ(any_sequence_stream_status::end_of_stream, status);
}

TEST_F(any_sequence_test, builtin_stream_close) {
    mock_any_sequence_stream stream{
        any_sequence{make_any_int64(1)},
        any_sequence{make_any_int64(2)}
    };

    // consume one row
    any_sequence seq{};
    (void) stream.next(seq, std::nullopt);

    // close the stream
    stream.close();

    // after close, next should return end_of_stream
    auto status = stream.next(seq, std::nullopt);
    EXPECT_EQ(any_sequence_stream_status::end_of_stream, status);
}

TEST_F(any_sequence_test, builtin_stream_reset) {
    mock_any_sequence_stream stream{
        any_sequence{make_any_int64(1)},
        any_sequence{make_any_int64(2)}
    };

    // consume all rows
    any_sequence seq{};
    (void) stream.next(seq, std::nullopt);
    (void) stream.next(seq, std::nullopt);

    auto status = stream.next(seq, std::nullopt);
    EXPECT_EQ(any_sequence_stream_status::end_of_stream, status);

    // reset and re-read
    stream.reset();
    EXPECT_EQ(0, stream.position());

    status = stream.next(seq, std::nullopt);
    EXPECT_EQ(any_sequence_stream_status::ok, status);
    EXPECT_EQ(1, seq[0].to<std::int64_t>());
}

}  // namespace jogasaki::data
