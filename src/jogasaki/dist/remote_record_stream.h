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
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <tateyama/proto/distributed_query/remote_catalog.pb.h>

#include <jogasaki/meta/record_meta.h>

#include <jogasaki/status.h>

namespace jogasaki::dist {
inline constexpr std::size_t remote_message_limit = std::size_t{64} * 1024 * 1024;
// The transport adapter sends this protobuf as one message. It never fragments records.
class remote_record_stream_writer {
public:
    using sender = std::function<status(tateyama::grpc::proto::RemoteProcessResult const&)>;
    explicit remote_record_stream_writer(sender send, std::size_t limit = remote_message_limit,
                                         std::function<bool()> cancelled = {});
    status append(std::string_view record);
    /** @brief send the current complete-record batch without closing the stream. */
    status flush();
    status finish();

private:
    sender send_;
    std::function<bool()> cancelled_;
    std::size_t limit_;
    std::size_t bytes_{};
    tateyama::grpc::proto::RemoteProcessResult batch_{};
    status failed_{status::ok};
    bool finished_{};
};
// Validate the whole batch before delivering any of its records to downstream.
class remote_record_stream_reader {
public:
    explicit remote_record_stream_reader(meta::record_meta const& meta, std::size_t limit = remote_message_limit)
        : meta_(std::addressof(meta)), limit_(limit) {}
    status accept(std::string_view bytes, std::function<status(std::string_view)> const& receive);
    [[nodiscard]] std::string const& diagnostic() const noexcept { return diagnostic_; }

private:
    meta::record_meta const* meta_;
    std::size_t limit_;
    std::uint64_t next_{};
    status failed_{status::ok};
    std::string diagnostic_{};
};
} // namespace jogasaki::dist
