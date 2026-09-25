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

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/udf/data/udf_any_sequence_stream.h>
#include <jogasaki/udf/data/udf_semantic_type.h>
#include <jogasaki/udf/generic_record.h>
#include <jogasaki/udf/generic_record_impl.h>

namespace jogasaki::testing {

using jogasaki::udf::data::udf_wire_kind;

std::string sequence_payload(std::size_t size) {
    std::string payload{};
    payload.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        payload.push_back(static_cast<char>(i));
    }
    return payload;
}

TEST(udf_any_sequence_stream_test, preserves_long_character_before_octet_field) {
    auto source = std::make_unique<plugin::udf::generic_record_stream_impl>();
    auto record = std::make_unique<plugin::udf::generic_record_impl>();

    std::string const tag(16, 'A');
    auto payload = sequence_payload(tag.size());

    plugin::udf::bytes_value bytes{};
    bytes.value = payload;
    record->add_string(tag);
    record->add_bytes(std::move(bytes));
    source->push(std::move(record));
    source->end_of_stream();

    mock_memory_resource resource{};
    udf::data::udf_any_sequence_stream stream{
        std::move(source),
        std::vector<udf_wire_kind>{udf_wire_kind::character, udf_wire_kind::octet},
        &resource,
    };

    data::any_sequence sequence{};
    ASSERT_EQ(data::any_sequence_stream_status::ok, stream.try_next(sequence));
    ASSERT_EQ(2U, sequence.size());

    auto text = sequence[0].to<accessor::text>();
    auto binary = sequence[1].to<accessor::binary>();
    EXPECT_EQ(std::string_view{tag}, static_cast<std::string_view>(text));
    EXPECT_EQ(std::string_view{payload}, static_cast<std::string_view>(binary));
}

TEST(udf_any_sequence_stream_test, preserves_long_octet_before_character_field) {
    auto source = std::make_unique<plugin::udf::generic_record_stream_impl>();
    auto record = std::make_unique<plugin::udf::generic_record_impl>();

    auto payload = sequence_payload(16);
    std::string const tag(16, 'B');

    plugin::udf::bytes_value bytes{};
    bytes.value = payload;
    record->add_bytes(std::move(bytes));
    record->add_string(tag);
    source->push(std::move(record));
    source->end_of_stream();

    mock_memory_resource resource{};
    udf::data::udf_any_sequence_stream stream{
        std::move(source),
        std::vector<udf_wire_kind>{udf_wire_kind::octet, udf_wire_kind::character},
        &resource,
    };

    data::any_sequence sequence{};
    ASSERT_EQ(data::any_sequence_stream_status::ok, stream.try_next(sequence));
    ASSERT_EQ(2U, sequence.size());

    auto binary = sequence[0].to<accessor::binary>();
    auto text = sequence[1].to<accessor::text>();
    EXPECT_EQ(std::string_view{payload}, static_cast<std::string_view>(binary));
    EXPECT_EQ(std::string_view{tag}, static_cast<std::string_view>(text));
}

TEST(udf_any_sequence_stream_test, preserves_multiple_long_variable_length_fields) {
    auto source = std::make_unique<plugin::udf::generic_record_stream_impl>();
    auto record = std::make_unique<plugin::udf::generic_record_impl>();

    std::string const first_text(16, 'A');
    std::string const second_text(16, 'B');
    auto first_binary = sequence_payload(16);
    std::string const second_binary(16, 'C');

    record->add_string(first_text);
    record->add_string(second_text);
    record->add_bytes(plugin::udf::bytes_value{first_binary});
    record->add_bytes(plugin::udf::bytes_value{second_binary});
    source->push(std::move(record));
    source->end_of_stream();

    mock_memory_resource resource{};
    udf::data::udf_any_sequence_stream stream{
        std::move(source),
        std::vector<udf_wire_kind>{
            udf_wire_kind::character,
            udf_wire_kind::character,
            udf_wire_kind::octet,
            udf_wire_kind::octet,
        },
        &resource,
    };

    data::any_sequence sequence{};
    ASSERT_EQ(data::any_sequence_stream_status::ok, stream.try_next(sequence));
    ASSERT_EQ(4U, sequence.size());

    auto actual_first_text = sequence[0].to<accessor::text>();
    auto actual_second_text = sequence[1].to<accessor::text>();
    auto actual_first_binary = sequence[2].to<accessor::binary>();
    auto actual_second_binary = sequence[3].to<accessor::binary>();
    EXPECT_EQ(std::string_view{first_text}, static_cast<std::string_view>(actual_first_text));
    EXPECT_EQ(std::string_view{second_text}, static_cast<std::string_view>(actual_second_text));
    EXPECT_EQ(std::string_view{first_binary}, static_cast<std::string_view>(actual_first_binary));
    EXPECT_EQ(std::string_view{second_binary}, static_cast<std::string_view>(actual_second_binary));
}

TEST(udf_any_sequence_stream_test, preserves_null_fields_for_all_output_types) {
    auto source = std::make_unique<plugin::udf::generic_record_stream_impl>();
    auto record = std::make_unique<plugin::udf::generic_record_impl>();

    record->add_bool_null();
    record->add_int4_null();
    record->add_int8_null();
    record->add_float_null();
    record->add_double_null();
    record->add_string_null();
    record->add_bytes_null();
    record->add_decimal_null();
    record->add_date_null();
    record->add_local_time_null();
    record->add_local_datetime_null();
    record->add_offset_datetime_null();
    record->add_blob_reference_null();
    record->add_clob_reference_null();
    source->push(std::move(record));
    source->end_of_stream();

    mock_memory_resource resource{};
    udf::data::udf_any_sequence_stream stream{
        std::move(source),
        std::vector<udf_wire_kind>{
            udf_wire_kind::boolean,
            udf_wire_kind::int4,
            udf_wire_kind::int8,
            udf_wire_kind::float4,
            udf_wire_kind::float8,
            udf_wire_kind::character,
            udf_wire_kind::octet,
            udf_wire_kind::decimal,
            udf_wire_kind::date,
            udf_wire_kind::time_of_day,
            udf_wire_kind::time_point,
            udf_wire_kind::time_point_with_time_zone,
            udf_wire_kind::blob,
            udf_wire_kind::clob,
        },
        &resource,
    };

    data::any_sequence sequence{};
    ASSERT_EQ(data::any_sequence_stream_status::ok, stream.try_next(sequence));
    ASSERT_EQ(14U, sequence.size());
    for (auto const& value : sequence) {
        EXPECT_TRUE(value.empty());
    }
    EXPECT_EQ(0U, resource.total_bytes_allocated_);
}

}  // namespace jogasaki::testing
