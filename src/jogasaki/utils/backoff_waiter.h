/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <thread>
#include <chrono>

namespace jogasaki::utils {

/**
 * @brief utility class to wait with exponential backoff
 */
class backoff_waiter {

public:
    ~backoff_waiter() = default;
    backoff_waiter(backoff_waiter const& other) = default;
    backoff_waiter& operator=(backoff_waiter const& other) = default;
    backoff_waiter(backoff_waiter&& other) noexcept = default;
    backoff_waiter& operator=(backoff_waiter&& other) noexcept = default;

    /**
     * @brief construct new object
     * @param initial_wait_ns the initial wait duration. Specify 0 to disable wait.
     * @param max_wait_ns the maximum wait duration
     */
    explicit backoff_waiter(
        std::size_t initial_wait_ns = 100UL * 1000UL,
        std::size_t max_wait_ns = 100UL * 1000UL * 1000UL
    ) :
        initial_wait_ns_(initial_wait_ns),
        max_wait_ns_(max_wait_ns),
        current_wait_ns_(initial_wait_ns)
    {}

    /**
     * @brief reset the state to initial
     */
    void reset() noexcept {
        current_wait_ns_ = initial_wait_ns_;
    }

    /**
     * @brief wait for current duration
     * @details wait for the current duration and then double it, with max limit given by `max_wait_ns_`.
     *
     */
    void operator()() {
        if(current_wait_ns_ == 0 || initial_wait_ns_ == 0) return;
        std::this_thread::sleep_for(std::chrono::nanoseconds{current_wait_ns_});
        current_wait_ns_ = std::min(current_wait_ns_*2, max_wait_ns_);
    }

private:
    std::size_t initial_wait_ns_{};
    std::size_t max_wait_ns_{};
    std::size_t current_wait_ns_{};

};


}


