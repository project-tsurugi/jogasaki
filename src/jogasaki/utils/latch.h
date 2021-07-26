/*
 * Copyright 2018-2020 tsurugi project.
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

#include <memory>
#include <mutex>
#include <condition_variable>

#include <jogasaki/utils/interference_size.h>

namespace jogasaki::utils {

/**
 * @brief latch to block current thread and wait for others
 * @details latch has two status open/close. It's created open. A thread can close it and wait on its reopen.
 * The other thread can open it to wake up the waiting thread and let it proceed.
 * This object assumes only two threads accessing simultaneously.
 */
class cache_align latch {
public:
    /**
     * @brief create new object
     */
    latch() noexcept = default;

    ~latch() = default;
    latch(latch const& other) = delete;
    latch& operator=(latch const& other) = delete;
    latch(latch&& other) noexcept = delete;
    latch& operator=(latch&& other) noexcept = delete;

    /**
     * @brief open the latch and notify the waiter to proceed
     * @note this function is thread-safe
     */
    void open();

    /**
     * @brief close the latch and wait for others to open it
     * @note this function is thread-safe
     */
    void wait();

    /**
     * @brief close the latch and wait for others to open it
     * @note this function is thread-safe
     */
    template <class Rep, class Period>
    bool wait(std::chrono::duration<Rep, Period> dur) {
        std::unique_lock lock{guard_};
        open_ = false;
        return cv_.wait_for(lock, dur, [&](){ return open_; });
    }

private:
    std::mutex guard_{};
    std::condition_variable cv_{};
    bool open_{true};
};

} // namespace
