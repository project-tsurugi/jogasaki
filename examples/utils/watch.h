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
#include<thread>

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
        std::scoped_lock lk{guard_};
        begin_ = Clock::now();
        initialize_this_thread();
    }
    bool wrap(std::size_t slot) {
        if (slot >= NUM_WRAPS) {
            std::abort();
        }
        initialize_this_thread();
        auto& time_slot = wraps_[std::this_thread::get_id()][slot];
        if (time_slot == Clock::time_point()) {
            std::scoped_lock lk{guard_};
            if (time_slot == Clock::time_point()) {
                time_slot = Clock::now();
                return true;
            }
        }
        std::abort();
        return false;
    }
    Clock::time_point base() {
        return begin_;
    }
    Clock::time_point view_first(std::size_t slot) {
        if (slot >= NUM_WRAPS) {
            return begin_;
        }
        Clock::time_point first{Clock::time_point::max()};
        for(auto& p : wraps_) {
            auto& arr = p.second;
            if (arr[slot] != Clock::time_point() && arr[slot] < first) {
                first = arr[slot];
            }
        }
        return first;
    }

    Clock::time_point view_last(std::size_t slot) {
        if (slot >= NUM_WRAPS) {
            return begin_;
        }
        Clock::time_point last{Clock::time_point::min()};
        for(auto& p : wraps_) {
            auto& arr = p.second;
            if (arr[slot] != Clock::time_point() && last < arr[slot]) {
                last = arr[slot];
            }
        }
        return last;
    }

    std::size_t duration(std::size_t end, std::size_t begin = -1) {
        return std::chrono::duration_cast<Duration>(view_last(end) - view_first(begin)).count();
    }

    std::size_t average_duration(std::size_t end, std::size_t begin = -1) {
        std::size_t count = 0;
        std::size_t total = 0;
        Clock::time_point fixed_begin = view_last(begin);
        Clock::time_point fixed_end = view_first(end);
        for(auto& p : wraps_) {
            auto& arr = p.second;
            if (arr[begin] == Clock::time_point() && arr[end] == Clock::time_point()) continue;
            auto e = arr[end] == Clock::time_point() ? fixed_end : arr[end];
            auto b = arr[begin] == Clock::time_point() ? fixed_begin : arr[begin];
            total += std::chrono::duration_cast<Duration>(e-b).count();
            ++count;
        }
        if (count == 0) return 0;
        return total / count;
    }

    void init(std::array<Clock::time_point, NUM_WRAPS>& wraps) {
        for(int i=0; i < NUM_WRAPS; ++i) {
            wraps[i] = Clock::time_point();
        }
    }
    void initialize_this_thread() {
        auto tid = std::this_thread::get_id();
        if(wraps_.count(tid) == 0) {
            init(wraps_[tid]);
        }
    }
private:
    std::chrono::time_point<Clock> begin_{};
    std::unordered_map<std::thread::id, std::array<Clock::time_point, NUM_WRAPS>> wraps_{};
    std::mutex guard_;
};

} // namespace
