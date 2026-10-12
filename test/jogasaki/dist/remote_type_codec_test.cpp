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
#include <gtest/gtest.h>

#include <takatori/type/bit.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/datetime_interval.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/value/bit.h>
#include <takatori/value/character.h>
#include <takatori/value/date.h>
#include <takatori/value/datetime_interval.h>
#include <takatori/value/decimal.h>
#include <takatori/value/octet.h>
#include <takatori/value/primitive.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/dist/remote_record_codec.h>
#include <jogasaki/dist/remote_type_codec.h>
#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>
#include <jogasaki/serializer/value_writer.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::testing {
namespace {
namespace t = takatori::type;
namespace v = takatori::value;
struct sample { std::shared_ptr<t::data const> type; std::unique_ptr<v::data> value; };
std::vector<sample> samples() {
    std::vector<sample> result{};
    result.push_back({std::make_shared<t::boolean>(), std::make_unique<v::boolean>(true)});
    result.push_back({std::make_shared<t::int4>(), std::make_unique<v::int4>(123456)});
    result.push_back({std::make_shared<t::int8>(), std::make_unique<v::int8>(123456789012LL)});
    result.push_back({std::make_shared<t::float4>(), std::make_unique<v::float4>(1.25F)});
    result.push_back({std::make_shared<t::float8>(), std::make_unique<v::float8>(1.125)});
    result.push_back({std::make_shared<t::decimal>(10, 2), std::make_unique<v::decimal>("12.34")});
    result.push_back({std::make_shared<t::character>(t::varying, 40), std::make_unique<v::character>("abc-日本語")});
    result.push_back({std::make_shared<t::octet>(t::varying, 40), std::make_unique<v::octet>(std::string{"a\0b", 3})});
    result.push_back({std::make_shared<t::date>(), std::make_unique<v::date>(takatori::datetime::date{2024, 1, 2})});
    result.push_back({std::make_shared<t::time_of_day>(), std::make_unique<v::time_of_day>(takatori::datetime::time_of_day{12, 34, 56})});
    result.push_back({std::make_shared<t::time_point>(), std::make_unique<v::time_point>(takatori::datetime::time_point{takatori::datetime::date{2024, 1, 2}, takatori::datetime::time_of_day{12, 34, 56}})});
    result.push_back({std::make_shared<t::unknown>(), std::make_unique<v::unknown>(v::unknown_kind::null)});
    return result;
}
struct buffer_writer {
    std::string bytes{};
    bool write(char const* data, std::size_t length) { bytes.append(data, length); return true; }
};
}
TEST(remote_type_codec_test, preserves_all_expression_types_and_literals) {
    for (auto const& sample : samples()) {
        auto type = dist::decode_remote_type(dist::encode_remote_type(*sample.type));
        ASSERT_TRUE(type) << *sample.type;
        EXPECT_EQ(*sample.type, *type);
        auto value = dist::decode_remote_literal(dist::encode_remote_literal(*sample.value), *type);
        ASSERT_TRUE(value) << *sample.value;
        EXPECT_EQ(*sample.value, *value);
    }
}
TEST(remote_type_codec_test, preserves_timezone_and_fixed_length_parameters) {
    for (auto const& type : std::vector<std::shared_ptr<t::data const>>{
        std::make_shared<t::time_point>(t::with_time_zone),
        std::make_shared<t::time_of_day>(t::with_time_zone),
        std::make_shared<t::character>(17), std::make_shared<t::octet>(9)}) {
        auto decoded = dist::decode_remote_type(dist::encode_remote_type(*type));
        ASSERT_TRUE(decoded);
        EXPECT_EQ(*type, *decoded);
    }
}
TEST(remote_type_codec_test, round_trips_every_supported_output_with_existing_row_encoding) {
    for (auto const& sample : samples()) {
        auto kind = sample.type->kind();
        if (kind == t::type_kind::int1 || kind == t::type_kind::int2 || kind == t::type_kind::bit || kind == t::type_kind::datetime_interval || kind == t::type_kind::unknown) continue;
        auto meta = std::make_shared<meta::record_meta>(std::vector<meta::field_type>{utils::type_for(*sample.type)}, boost::dynamic_bitset<std::uint64_t>{1, 1});
        buffer_writer buffer{};
        serializer::value_writer<buffer_writer> writer{buffer};
        writer.write_row_begin(1);
        buffer.bytes += dist::encode_remote_literal(*sample.value);
        data::small_record_store record{meta};
        std::string error{};
        ASSERT_TRUE(dist::decode_remote_record(buffer.bytes, *meta, record.ref(), error)) << *sample.type << ": " << error;
        EXPECT_EQ(buffer.bytes, dist::encode_remote_record(record.ref(), *meta));
        buffer.bytes.clear();
        writer.write_row_begin(1);
        writer.write_null();
        ASSERT_TRUE(dist::decode_remote_record(buffer.bytes, *meta, record.ref(), error)) << error;
        EXPECT_EQ(buffer.bytes, dist::encode_remote_record(record.ref(), *meta));
    }
}
TEST(remote_type_codec_test, rejects_invalid_type_and_trailing_literal_data) {
    proto::distributed_query::PortableType type{};
    type.set_kind("int8");
    type.set_length(3);
    EXPECT_FALSE(dist::decode_remote_type(type.SerializeAsString()));
    auto payload = dist::encode_remote_literal(v::int8{10});
    payload += payload;
    EXPECT_FALSE(dist::decode_remote_literal(payload, t::int8{}));
}
TEST(remote_type_codec_test, rejects_unsupported_types_and_literals_including_null) {
    for (auto const& type : std::vector<std::shared_ptr<t::data const>>{
        std::make_shared<t::int1>(), std::make_shared<t::int2>(),
        std::make_shared<t::bit>(t::varying, 9), std::make_shared<t::datetime_interval>()}) {
        EXPECT_THROW((void) dist::encode_remote_type(*type), std::invalid_argument);
        proto::distributed_query::PortableType wire{};
        wire.set_kind(std::string{t::to_string_view(type->kind())});
        EXPECT_FALSE(dist::decode_remote_type(wire.SerializeAsString()));
        auto null = dist::encode_remote_literal(v::unknown{v::unknown_kind::null});
        EXPECT_FALSE(dist::decode_remote_literal(null, *type));
    }
    EXPECT_THROW((void) dist::encode_remote_literal(v::bit{"1010101"}), std::invalid_argument);
    EXPECT_THROW((void) dist::encode_remote_literal(v::datetime_interval{takatori::datetime::datetime_interval{}}), std::invalid_argument);
}

}
