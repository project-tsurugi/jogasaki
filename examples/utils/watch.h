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

    /**
     * @brief identifier for the point in source code
     */
    using point_in_code = std::size_t;

    /**
     * @brief virtual id for the very beginning of this watch
     */
    static constexpr point_in_code npos = static_cast<point_in_code>(-1);

    /**
     * @brief the number of points in source code to be recorded
     */
    const static int num_points = 10;

    watch() {
        restart();
    }

    void restart() {
        std::scoped_lock lk{guard_};
        begin_ = Clock::now();
        initialize_this_thread();
    }

    bool set_point(point_in_code loc) {
        if (loc >= num_points) {
            std::abort();
        }
        std::scoped_lock lk{guard_};
        initialize_this_thread();
        auto& time_slot = records_[std::this_thread::get_id()][loc];
        if (time_slot == Clock::time_point()) {
            time_slot = Clock::now();
            return true;
        }
        return false;
    }

    Clock::time_point base() {
        return begin_;
    }

    /**
     * @brief retrieve the time when first comes at the point of code
     */
    Clock::time_point view_first(point_in_code loc) {
        if (loc == npos) {
            return begin_;
        }
        Clock::time_point first{Clock::time_point::max()};
        for(auto& p : records_) {
            auto& arr = p.second;
            if (arr[loc] != Clock::time_point() && arr[loc] < first) {
                first = arr[loc];
            }
        }
        return first;
    }

    /**
     * @brief retrieve the time when last leaves the point of code
     */
    Clock::time_point view_last(point_in_code loc) {
        if (loc == npos) {
            return begin_;
        }
        Clock::time_point last{Clock::time_point::min()};
        for(auto& p : records_) {
            auto& arr = p.second;
            if (arr[loc] != Clock::time_point() && last < arr[loc]) {
                last = arr[loc];
            }
        }
        return last;
    }

    /**
     * @brief calculate duration between two time point
     * @param begin time point defining beginning of the interval
     * @param end time point defining end of the interval
     * @param complemental if false, interval begins when first thread comes and ends when last leaves
     * if true, interval begins when last thread comes and ends when first leaves
     * @return duration
     */
    std::size_t duration(point_in_code begin, point_in_code end, bool complemental = false) {
        if (!complemental) {
            return std::chrono::duration_cast<Duration>(view_last(end) - view_first(begin)).count();
        }
        return std::chrono::duration_cast<Duration>(view_first(end) - view_last(begin)).count();
    }

    std::size_t average_duration(point_in_code begin, point_in_code end, bool complemental = false) {
        std::size_t count = 0;
        std::size_t total = 0;
        Clock::time_point fixed_begin = complemental ? view_last(begin) : view_first(begin);
        Clock::time_point fixed_end = complemental ? view_first(end) : view_last(end);
        for(auto& p : records_) {
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

private:
    std::chrono::time_point<Clock> begin_{};
    std::unordered_map<std::thread::id, std::array<Clock::time_point, num_points>> records_{};
    std::mutex guard_{};

    void init(std::array<Clock::time_point, num_points>& records) {
        for(auto& s : records) {
            s = Clock::time_point();
        }
    }

    void initialize_this_thread() {
        auto tid = std::this_thread::get_id();
        if(records_.count(tid) == 0) {
            init(records_[tid]);
        }
    }
};

} // namespace
