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

#include <thread>
#include <chrono>

namespace jogasaki::utils {

/**
 * @brief utility class to wait with exponential backoff
 */
class backoff_timer {

public:

    using Clock = std::chrono::high_resolution_clock;

    ~backoff_timer() = default;
    backoff_timer(backoff_timer const& other) = delete;
    backoff_timer& operator=(backoff_timer const& other) = delete;
    backoff_timer(backoff_timer&& other) noexcept = delete;
    backoff_timer& operator=(backoff_timer&& other) noexcept = delete;

    /**
     * @brief construct new object
     * @param initial_wait_ns the initial wait duration. Specify 0 to disable wait.
     * @param max_wait_ns the maximum wait duration
     */
    explicit backoff_timer(
        std::size_t initial_wait_ns = 100UL * 1000UL,
        std::size_t max_wait_ns = 100UL * 1000UL * 1000UL
    ) :
        initial_wait_ns_(initial_wait_ns),
        max_wait_ns_(max_wait_ns),
        current_wait_ns_(initial_wait_ns),
        begin_(Clock::now())
    {}

    /**
     * @brief reset the state to initial
     */
    void reset() noexcept {
        current_wait_ns_ = initial_wait_ns_;
        begin_ = Clock::now();
    }

    /**
     * @brief wait for current duration
     * @details wait for the current duration and then double it, with max limit given by `max_wait_ns_`.
     *
     */
    bool operator()() {
        if(current_wait_ns_ == 0 || initial_wait_ns_ == 0) return true;
        auto now = Clock::now();
        if(std::chrono::duration_cast<std::chrono::nanoseconds>(now - begin_).count() > static_cast<std::int64_t>(current_wait_ns_)) {
            current_wait_ns_ = std::min(current_wait_ns_*2, max_wait_ns_);
            begin_ = Clock::now();
            return true;
        }
        return false;
    }

private:
    std::size_t initial_wait_ns_{};
    std::size_t max_wait_ns_{};
    std::atomic_size_t current_wait_ns_{};
    std::chrono::time_point<Clock> begin_{};

};


}


