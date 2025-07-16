/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>

#include <jogasaki/utils/interference_size.h>

namespace jogasaki::utils {

/**
 * @brief latch to block current thread and wait for others
 * @details latch has three status open/closed/released. It's created open by default.
 * A thread can close it and wait on its release.
 * The other thread can release it to wake up the waiting thread and let it proceed.
 * This object assumes only two threads accessing simultaneously.
 * Releasing latch is a idempotent one-way operation, so released latch cannot be closed even if release() happens before
 * wait(), then wait() doesn't actually wait.
 * Exceptionally reset() can reset the status to open so that threads re-use the latch.
 */
class cache_align latch {
public:
    /**
     * @brief create new object
     * @param released if true, create the latch in released state.
     */
    explicit latch(bool released = false) noexcept;

    ~latch() = default;
    latch(latch const& other) = delete;
    latch& operator=(latch const& other) = delete;
    latch(latch&& other) noexcept = delete;
    latch& operator=(latch&& other) noexcept = delete;

    /**
     * @brief release the latch and unblock the waiter to proceed
     * @note this function is thread-safe
     */
    void release();

    /**
     * @brief close the latch and wait for release. If it's already released, this call is no-op.
     * @note this function is thread-safe
     */
    void wait();

    /**
     * @brief close the latch and wait for release within given duration. If it's already released, this call is no-op.
     * @note this function is thread-safe
     * @returns true if the latch is open within the duration
     * @returns false if time-out occurs
     */
    template <class Rep, class Period>
    bool wait(std::chrono::duration<Rep, Period> dur) {
        std::unique_lock lock{guard_};
        if (done_) return true; // already opened, should not wait
        open_ = false;
        return cv_.wait_for(lock, dur, [&](){ return open_; });
    }

    /**
     * @brief reset the latch state to open to re-use
     * @note this function is thread-safe
     */
    void reset();

private:
    std::mutex guard_{};
    std::condition_variable cv_{};
    bool open_{true};
    bool done_{false};
};

} // namespace
