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

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/dist/remote_record_codec.h>
#include <jogasaki/dist/remote_record_stream.h>

namespace jogasaki::testing {
namespace {
using batch = tateyama::grpc::proto::RemoteProcessResult;
meta::record_meta int_meta() {
    return {std::vector<meta::field_type>{meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>}}, boost::dynamic_bitset<std::uint64_t>(1)};
}
std::string row(std::int64_t value) {
    auto meta = std::make_shared<meta::record_meta>(int_meta());
    data::small_record_store record{meta};
    record.ref().set_value<std::int64_t>(meta->value_offset(0), value);
    return dist::encode_remote_record(record.ref(), *meta);
}
}
TEST(remote_record_stream_test, preserves_order_duplicates_and_complete_records) {
    std::vector<batch> sent{};
    dist::remote_record_stream_writer writer{[&](auto const& message) { sent.push_back(message); return status::ok; }, 12};
    std::vector<std::string> rows{row(7), row(7), row(42), row(9), row(7)};
    for (auto const& value : rows) ASSERT_EQ(status::ok, writer.append(value));
    ASSERT_EQ(status::ok, writer.finish());
    ASSERT_GT(sent.size(), 1);
    auto meta = int_meta();
    dist::remote_record_stream_reader reader{meta, 12};
    std::vector<std::string> received{};
    for (std::size_t i = 0; i < sent.size(); ++i) {
        EXPECT_EQ(i, sent[i].sequence_number());
        EXPECT_LE(sent[i].ByteSizeLong(), 12);
        ASSERT_EQ(status::ok, reader.accept(sent[i].SerializeAsString(), [&](auto value) { received.emplace_back(value); return status::ok; }));
    }
    EXPECT_EQ(rows, received);
    EXPECT_EQ(status::err_invalid_state, writer.append(row(1)));
}
TEST(remote_record_stream_test, explicit_flush_keeps_stream_open_and_contiguous) {
    std::vector<std::string> records{};
    std::vector<std::uint64_t> sequences{};
    dist::remote_record_stream_writer writer{[&](auto const& batch) {
        sequences.push_back(batch.sequence_number());
        for (auto const& record : batch.record_list()) records.push_back(record);
        return status::ok;
    }};
    ASSERT_EQ(status::ok, writer.append("first"));
    ASSERT_EQ(status::ok, writer.flush());
    EXPECT_EQ((std::vector<std::string>{"first"}), records);
    ASSERT_EQ(status::ok, writer.append("second"));
    ASSERT_EQ(status::ok, writer.finish());
    EXPECT_EQ((std::vector<std::uint64_t>{0, 1}), sequences);
    EXPECT_EQ((std::vector<std::string>{"first", "second"}), records);
    EXPECT_EQ(status::err_invalid_state, writer.flush());
}

TEST(remote_record_stream_test, empty_result_sends_no_message) {
    int calls{};
    dist::remote_record_stream_writer writer{[&](auto const&) { ++calls; return status::ok; }};
    EXPECT_EQ(status::ok, writer.finish());
    EXPECT_EQ(0, calls);
}
TEST(remote_record_stream_test, protobuf_overhead_counts_at_exact_message_limit) {
    auto value = row(1);
    batch expected{}; expected.add_record_list(value);
    int calls{};
    dist::remote_record_stream_writer exact{[&](auto const& message) { ++calls; EXPECT_EQ(expected.ByteSizeLong(), message.ByteSizeLong()); return status::ok; }, expected.ByteSizeLong()};
    EXPECT_EQ(status::ok, exact.append(value)); EXPECT_EQ(status::ok, exact.finish()); EXPECT_EQ(1, calls);
    dist::remote_record_stream_writer small{[&](auto const&) { ++calls; return status::ok; }, expected.ByteSizeLong() - 1};
    EXPECT_EQ(status::err_resource_limit_reached, small.append(value));
    EXPECT_EQ(status::err_resource_limit_reached, small.finish()); EXPECT_EQ(1, calls);
}
TEST(remote_record_stream_test, sequence_varint_growth_is_included) {
    std::vector<batch> sent{};
    auto value = row(1);
    batch base{}; base.set_sequence_number(128); base.add_record_list(value);
    dist::remote_record_stream_writer writer{[&](auto const& message) { sent.push_back(message); return status::ok; }, base.ByteSizeLong()};
    for (int i = 0; i < 300; ++i) ASSERT_EQ(status::ok, writer.append(value));
    ASSERT_EQ(status::ok, writer.finish());
    EXPECT_GE(sent.size(), 129);
    for (auto const& message : sent) EXPECT_LE(message.ByteSizeLong(), base.ByteSizeLong());
}
TEST(remote_record_stream_test, exact_64_mib_serialized_batch_is_accepted) {
    std::string value(dist::remote_message_limit - 5, 'x');
    int calls{};
    dist::remote_record_stream_writer writer{[&](auto const& message) {
        ++calls; EXPECT_EQ(dist::remote_message_limit, message.ByteSizeLong()); EXPECT_EQ(1, message.record_list_size()); return status::ok;
    }};
    EXPECT_EQ(status::ok, writer.append(value)); EXPECT_EQ(status::ok, writer.finish()); EXPECT_EQ(1, calls);
}
TEST(remote_record_stream_test, real_64_mib_limit_rejects_single_record_without_sending) {
    std::string value(dist::remote_message_limit, 'x');
    int calls{};
    dist::remote_record_stream_writer writer{[&](auto const&) { ++calls; return status::ok; }};
    EXPECT_EQ(status::err_resource_limit_reached, writer.append(value)); EXPECT_EQ(0, calls);
    auto meta = int_meta(); dist::remote_record_stream_reader reader{meta};
    value.push_back('x');
    EXPECT_EQ(status::err_resource_limit_reached, reader.accept(value, [](auto) { return status::ok; }));
}
TEST(remote_record_stream_test, rejects_malformed_missing_and_duplicate_sequence_before_delivery) {
    auto meta = int_meta();
    for (auto sequence : {1U, 9U}) {
        dist::remote_record_stream_reader reader{meta}; batch message{};
        message.set_sequence_number(sequence); message.add_record_list(row(1));
        int delivered{};
        EXPECT_EQ(status::err_invalid_argument, reader.accept(message.SerializeAsString(), [&](auto) { ++delivered; return status::ok; }));
        EXPECT_EQ(0, delivered);
    }
    dist::remote_record_stream_reader reader{meta}; batch first{}; first.add_record_list(row(1));
    EXPECT_EQ(status::ok, reader.accept(first.SerializeAsString(), [](auto) { return status::ok; }));
    EXPECT_EQ(status::err_invalid_argument, reader.accept(first.SerializeAsString(), [](auto) { return status::ok; }));
    dist::remote_record_stream_reader malformed{meta};
    EXPECT_EQ(status::err_invalid_argument, malformed.accept("\xff", [](auto) { return status::ok; }));
}
TEST(remote_record_stream_test, incomplete_or_wrong_width_record_is_not_delivered) {
    auto meta = int_meta();
    for (auto value : {row(1).substr(0, 1), row(1) + "trailing", std::string{}}) {
        dist::remote_record_stream_reader reader{meta}; batch message{};
        message.add_record_list(row(2)); message.add_record_list(value);
        int delivered{};
        EXPECT_EQ(status::err_invalid_argument, reader.accept(message.SerializeAsString(), [&](auto) { ++delivered; return status::ok; }));
        EXPECT_EQ(0, delivered);
    }
}
TEST(remote_record_stream_test, cancellation_and_failed_sink_stop_output) {
    bool cancel{}; int calls{};
    dist::remote_record_stream_writer writer{[&](auto const&) { ++calls; return status::ok; }, 12, [&] { return cancel; }};
    EXPECT_EQ(status::ok, writer.append(row(1))); cancel = true;
    EXPECT_EQ(status::request_canceled, writer.finish()); EXPECT_EQ(0, calls);
    dist::remote_record_stream_writer failed{[](auto const&) { return status::err_io_error; }};
    EXPECT_EQ(status::ok, failed.append(row(1))); EXPECT_EQ(status::err_io_error, failed.finish());
    EXPECT_EQ(status::err_io_error, failed.append(row(2)));
    auto meta = int_meta(); dist::remote_record_stream_reader reader{meta}; batch message{}; message.add_record_list(row(1));
    EXPECT_EQ(status::request_canceled, reader.accept(message.SerializeAsString(), [](auto) { return status::request_canceled; }));
    EXPECT_EQ(status::request_canceled, reader.accept(message.SerializeAsString(), [](auto) { return status::ok; }));
}

TEST(remote_record_stream_test, rejects_missing_sender_and_invalid_limits) {
    EXPECT_THROW((void)dist::remote_record_stream_writer({}, 12), std::invalid_argument);
    auto send = [](auto const&) { return status::ok; };
    EXPECT_THROW((void)dist::remote_record_stream_writer(send, 0), std::invalid_argument);
    EXPECT_THROW((void)dist::remote_record_stream_writer(send, dist::remote_message_limit + 1), std::invalid_argument);
}
TEST(remote_record_stream_test, sender_failure_is_sticky_across_all_entry_points) {
    std::size_t calls{};
    dist::remote_record_stream_writer writer{[&](auto const&) { ++calls; return status::err_io_error; }};
    ASSERT_EQ(status::ok, writer.append(row(205)));
    EXPECT_EQ(status::err_io_error, writer.flush());
    EXPECT_EQ(status::err_io_error, writer.append(row(308)));
    EXPECT_EQ(status::err_io_error, writer.flush());
    EXPECT_EQ(status::err_io_error, writer.finish());
    EXPECT_EQ(1, calls);
}
TEST(remote_record_stream_test, cancellation_during_append_is_sticky_and_does_not_send) {
    std::size_t calls{};
    dist::remote_record_stream_writer writer{[&](auto const&) { ++calls; return status::ok; }, 12, [] { return true; }};
    EXPECT_EQ(status::request_canceled, writer.append(row(205)));
    EXPECT_EQ(status::request_canceled, writer.flush());
    EXPECT_EQ(status::request_canceled, writer.finish());
    EXPECT_EQ(0, calls);
}
TEST(remote_record_stream_test, reader_callback_failure_stops_remaining_rows_and_later_batches) {
    auto meta = int_meta();
    dist::remote_record_stream_reader reader{meta};
    batch message{};
    for (auto value : {101, 205, 308}) message.add_record_list(row(value));
    std::vector<std::string> delivered{};
    EXPECT_EQ(status::err_io_error, reader.accept(message.SerializeAsString(), [&](auto value) {
        delivered.emplace_back(value);
        return delivered.size() == 2 ? status::err_io_error : status::ok;
    }));
    EXPECT_EQ((std::vector<std::string>{row(101), row(205)}), delivered);
    message.set_sequence_number(1);
    EXPECT_EQ(status::err_io_error, reader.accept(message.SerializeAsString(), [&](auto) {
        ADD_FAILURE() << "failed reader must not deliver another row";
        return status::ok;
    }));
}
TEST(remote_record_stream_test, reader_rejects_missing_callback_before_delivery_and_keeps_error) {
    auto meta = int_meta();
    dist::remote_record_stream_reader reader{meta}; batch message{}; message.add_record_list(row(205));
    EXPECT_EQ(status::err_invalid_argument, reader.accept(message.SerializeAsString(), {}));
    EXPECT_FALSE(reader.diagnostic().empty());
    EXPECT_EQ(status::err_invalid_argument, reader.accept(message.SerializeAsString(), [](auto) {
        ADD_FAILURE() << "failed reader must not deliver rows"; return status::ok;
    }));
}

}
