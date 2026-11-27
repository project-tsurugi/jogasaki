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
#include "remote_record_stream.h"

#include <limits>
#include <stdexcept>
#include <utility>

#include <jogasaki/data/small_record_store.h>

#include "remote_record_codec.h"

namespace jogasaki::dist {

static std::size_t varint_size(std::uint64_t n) {
    std::size_t result = 1;
    while (n >= 128) {
        n >>= 7U;
        ++result;
    }
    return result;
}

static std::size_t sequence_size(std::uint64_t n) { return n == 0 ? 0 : 1 + varint_size(n); }

remote_record_stream_writer::remote_record_stream_writer(sender send, std::size_t limit,
                                                         std::function<bool()> cancelled)
    : send_(std::move(send)), cancelled_(std::move(cancelled)), limit_(limit) {
    if (!send_ || limit == 0 || limit > remote_message_limit) {
        throw std::invalid_argument("invalid record stream binding or limit");
    }
}
status remote_record_stream_writer::append(std::string_view record) {
    if (failed_ != status::ok) {
        return failed_;
    }
    if (finished_) {
        return status::err_invalid_state;
    }
    if (cancelled_ && cancelled_()) {
        return failed_ = status::request_canceled;
    }
    if (record.size() > limit_) {
        return failed_ = status::err_resource_limit_reached;
    }
    auto size = 1 + varint_size(record.size()) + record.size();
    if (size + sequence_size(batch_.sequence_number()) > limit_) {
        return failed_ = status::err_resource_limit_reached;
    }
    if (bytes_ + size > limit_) {
        if (auto st = flush(); st != status::ok) {
            return st;
        }
        if (bytes_ + size > limit_) {
            return failed_ = status::err_resource_limit_reached;
        }
    }
    batch_.add_record_list(record.data(), record.size());
    bytes_ += size;
    return status::ok;
}
status remote_record_stream_writer::flush() {
    if (failed_ != status::ok) return failed_;
    if (finished_) return status::err_invalid_state;
    if (batch_.record_list().empty()) {
        return status::ok;
    }
    if (cancelled_ && cancelled_()) {
        return failed_ = status::request_canceled;
    }
    if (auto st = send_(batch_); st != status::ok) {
        return failed_ = st;
    }
    if (batch_.sequence_number() == std::numeric_limits<std::uint64_t>::max()) {
        return failed_ = status::err_resource_limit_reached;
    }
    batch_.set_sequence_number(batch_.sequence_number() + 1);
    batch_.clear_record_list();
    bytes_ = sequence_size(batch_.sequence_number());
    return status::ok;
}
status remote_record_stream_writer::finish() {
    if (failed_ != status::ok) {
        return failed_;
    }
    if (finished_) {
        return status::err_invalid_state;
    }
    if (cancelled_ && cancelled_()) {
        return failed_ = status::request_canceled;
    }
    auto st = flush();
    finished_ = true;
    return st;
}
status remote_record_stream_reader::accept(std::string_view bytes,
                                           std::function<status(std::string_view)> const& receive) {
    if (failed_ != status::ok) {
        return failed_;
    }
    tateyama::grpc::proto::RemoteProcessResult batch{};
    if (bytes.size() > limit_ || bytes.size() > remote_message_limit) {
        diagnostic_ = "remote record batch exceeds message limit";
        return failed_ = status::err_resource_limit_reached;
    }
    if (!receive || !batch.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())) ||
        batch.sequence_number() != next_) {
        diagnostic_ = "invalid remote record batch or sequence";
        return failed_ = status::err_invalid_argument;
    }
    auto meta = std::make_shared<meta::record_meta>(*meta_);
    data::small_record_store record{meta};
    for (auto const& row : batch.record_list()) {
        if (!decode_remote_record(row, *meta_, record.ref(), diagnostic_)) {
            return failed_ = status::err_invalid_argument;
        }
    }
    for (auto const& row : batch.record_list()) {
        if (auto st = receive(row); st != status::ok) {
            return failed_ = st;
        }
    }
    ++next_;
    return status::ok;
}
} // namespace jogasaki::dist
