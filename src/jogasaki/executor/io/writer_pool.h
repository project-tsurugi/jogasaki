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
#pragma once

#include <cstddef>
#include <tbb/concurrent_queue.h>

#include <jogasaki/executor/io/writer_seat.h>

namespace jogasaki::executor::io {

class record_channel;

/**
 * @brief a fixed-capacity FIFO pool of writer seats
 * @details The pool maintains a fixed number of seats created during
 * construction.
 * @note this object is thread-safe and can be accessed from multiple threads concurrently.
 */
class writer_pool {
public:
    /**
     * @brief construct pool with given capacity
     * @param channel the record channel used to materialize writers
     * @param capacity the maximum number of seats in the pool
     */
    writer_pool(record_channel& channel, std::size_t capacity);

    ~writer_pool() = default;

    writer_pool(writer_pool const& other) = delete;
    writer_pool& operator=(writer_pool const& other) = delete;
    writer_pool(writer_pool&& other) noexcept = delete;
    writer_pool& operator=(writer_pool&& other) noexcept = delete;

    /**
     * @brief try to acquire a reserved seat from the pool
     * @param out the reserved seat that will receive the reserved seat state when available. Any
     * existing seat state in out will be move-assigned from the pool, including reserved status and
     * the lazily created writer (if present). When acquisition fails, out remains unchanged.
     * @return true when a seat was provided, false when pool empty
     */
    bool acquire(writer_seat& out) noexcept;

    /**
     * @brief return a previously acquired seat back to the pool
     * @details the seat together with its writer (if any) is returned to the pool and becomes
     * available for future acquire() calls. Ownership is moved back into the pool's internal seat
     * instance so that the caller's seat becomes non-reserved and empty.
     * @param seat the seat to return. After this call, the parameter will be non-reserved seat and
     * hold no writer.
     */
    void release(writer_seat&& seat) noexcept;

    /**
     * @brief release any resource held by the pool
     * @details all the seats in the pool are cleared and any writers held by them are released.
     * @note after this call, the pool can no longer be used to acquire seats. Otherwise, behavior is undefined.
     */
    void release_pool();

    /**
     * @brief total capacity of the pool
     */
    [[nodiscard]] std::size_t capacity() const noexcept;

private:
    record_channel* channel_{};
    std::size_t capacity_{};
    tbb::concurrent_queue<writer_seat> queue_{};
};

}  // namespace jogasaki::executor::io
