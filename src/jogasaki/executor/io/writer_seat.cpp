/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/executor/io/writer_seat.h>

#include <utility>

#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/assert.h>

namespace jogasaki::executor::io {

writer_seat::writer_seat(writer_seat&& other) noexcept:
    channel_(other.channel_),
    writer_(std::move(other.writer_)),
    reserved_(other.reserved_)
{
    other.channel_ = nullptr;
    other.writer_ = nullptr;
    other.reserved_ = false;
}

writer_seat& writer_seat::operator=(writer_seat&& other) noexcept {
    if (this != std::addressof(other)) {
        channel_ = other.channel_;
        writer_ = std::move(other.writer_);
        reserved_ = other.reserved_;
        other.channel_ = nullptr;
        other.writer_ = nullptr;
        other.reserved_ = false;
    }
    return *this;
}

bool writer_seat::reserved() const noexcept {
    return reserved_;
}

std::shared_ptr<record_writer> const& writer_seat::writer() {
    assert_with_exception(reserved_, reserved_);
    assert_with_exception(channel_ != nullptr, channel_);

    if (! writer_) {
        auto res = channel_->acquire(writer_);
        // acquire never fails here because reserved seat ensures that a writer can be acquired
        assert_with_exception(res == status::ok, res);
    }
    return writer_;
}

bool writer_seat::has_writer() const noexcept {
    return writer_ != nullptr;
}

}  // namespace jogasaki::executor::io
