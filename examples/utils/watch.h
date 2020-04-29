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

namespace jogasaki::utils {

class watch {
public:
    using Clock = std::chrono::steady_clock;
    using Duration = std::chrono::milliseconds;
    const static int NUM_WRAPS = 10;
    watch() {
        restart();
    }
    void restart() {
        begin_ = Clock::now();
        for(int i=0; i < NUM_WRAPS; ++i) {
            wraps_[i] = Clock::time_point();
        }
    }
    bool wrap(std::size_t slot) {
        if (slot >= NUM_WRAPS) {
            std::abort();
        }
        if (wraps_[slot] == Clock::time_point()) {
            std::scoped_lock lk{guard_};
            if (wraps_[slot] == Clock::time_point()) {
                wraps_[slot] = Clock::now();
                return true;
            }
        }
        return false;
    }
    Clock::time_point base() {
        return begin_;
    }
    Clock::time_point view(std::size_t slot) {
        if (slot >= NUM_WRAPS) {
            return begin_;
        }
        return wraps_[slot];
    }
    std::size_t duration(std::size_t end, std::size_t begin = -1) {
        return std::chrono::duration_cast<Duration>(view(end) - view(begin)).count();
    }

private:
    std::chrono::time_point<Clock> begin_{};
    std::array<Clock::time_point, NUM_WRAPS> wraps_{};
    std::mutex guard_;
};

} // namespace
