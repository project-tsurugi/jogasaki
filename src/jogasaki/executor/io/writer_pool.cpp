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
#include <jogasaki/executor/io/writer_pool.h>

#include <utility>

#include <jogasaki/executor/io/record_channel.h>

namespace jogasaki::executor::io {

writer_pool::writer_pool(record_channel& channel, std::size_t capacity):
    channel_(std::addressof(channel)),
    capacity_(capacity)
{
    for (std::size_t i = 0; i < capacity_; ++i) {
        queue_.push(writer_seat{channel_, true});
    }
}

bool writer_pool::acquire(writer_seat& out) noexcept {  // tbb queue and writer_seat won't throw, so we can noexcept
    writer_seat seat{};
    if (! queue_.try_pop(seat)) {
        return false;
    }
    out = std::move(seat);
    return true;
}

void writer_pool::release(writer_seat&& seat) noexcept {
    queue_.push(std::move(seat));
}

void writer_pool::release_pool() {
    // Pop and destroy all seats currently in the queue so any held writers are released.
    writer_seat seat{};
    while (queue_.try_pop(seat)) {
        if (seat.has_writer()) {
            // obtain the writer and return it to its owner via record_writer::release()
            auto const& wrt = seat.writer();
            wrt->flush();
            wrt->release();
        }
        // reset local seat for next iteration
        seat = writer_seat{};
    }

    // Drop the channel reference to indicate the pool no longer materializes writers.
    channel_ = nullptr;
}

[[nodiscard]] std::size_t writer_pool::capacity() const noexcept {
    return capacity_;
}

}  // namespace jogasaki::executor::io

