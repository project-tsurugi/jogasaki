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
#include "watch.h"

#include <chrono>

namespace jogasaki::utils {

watch::watch() noexcept {
    begin_ = Clock::now();
}

void watch::restart() {
    std::scoped_lock lk{guard_};
    begin_ = Clock::now();
}

bool watch::set_point(watch::point_in_code loc, watch::worker_id worker) {
    if (loc >= num_points) {
        std::abort();
    }
    std::scoped_lock lk{guard_};
    initialize_worker(worker);
    auto& time_slot = records_[worker][loc];
    if (time_slot == Clock::time_point()) {
        time_slot = Clock::now();
        return true;
    }
    return false;
}

watch::Clock::time_point watch::base() {
    return begin_;
}

watch::Clock::time_point watch::view_first(watch::point_in_code loc) {
    if (loc == npos) {
        return begin_;
    }
    Clock::time_point first{Clock::time_point::max()};
    for(auto& p : records_) {
        auto& arr = p.second;
        if (arr[loc] != Clock::time_point() && arr[loc] < first) { //NOLINT
            first = arr[loc]; //NOLINT
        }
    }
    return first;
}

watch::Clock::time_point watch::view_last(watch::point_in_code loc) {
    if (loc == npos) {
        return begin_;
    }
    Clock::time_point last{Clock::time_point::min()};
    for(auto& p : records_) {
        auto& arr = p.second;
        if (arr[loc] != Clock::time_point() && last < arr[loc]) { //NOLINT
            last = arr[loc]; //NOLINT
        }
    }
    return last;
}

std::size_t watch::duration(watch::point_in_code begin, watch::point_in_code end, bool complementary) {
    if (!complementary) {
        return std::chrono::duration_cast<Duration>(view_last(end) - view_first(begin)).count();
    }
    return std::chrono::duration_cast<Duration>(view_first(end) - view_last(begin)).count();
}

std::size_t watch::average_duration(watch::point_in_code begin, watch::point_in_code end) {
    std::size_t count = 0;
    std::size_t total = 0;
    Clock::time_point fixed_begin = view_first(begin);
    Clock::time_point fixed_end = view_last(end);
    for(auto& p : records_) {
        auto& arr = p.second;
        if (arr[begin] == Clock::time_point() && arr[end] == Clock::time_point()) continue; //NOLINT
        auto e = arr[end] == Clock::time_point() ? fixed_end : arr[end]; //NOLINT
        auto b = arr[begin] == Clock::time_point() ? fixed_begin : arr[begin]; //NOLINT
        total += std::chrono::duration_cast<Duration>(e-b).count();
        ++count;
    }
    if (count == 0) return 0;
    return total / count;
}

std::unique_ptr<std::vector<std::size_t>> watch::durations(watch::point_in_code begin, watch::point_in_code end) {
    auto results = std::make_unique<std::vector<std::size_t>>();
    Clock::time_point fixed_begin = view_first(begin);
    Clock::time_point fixed_end = view_last(end);
    for(auto& p : records_) {
        auto& arr = p.second;
        if (arr[begin] == Clock::time_point() && arr[end] == Clock::time_point()) continue; //NOLINT
        auto e = arr[end] == Clock::time_point() ? fixed_end : arr[end]; //NOLINT
        auto b = arr[begin] == Clock::time_point() ? fixed_begin : arr[begin]; //NOLINT
        results->emplace_back(std::chrono::duration_cast<Duration>(e-b).count());
    }
    return results;
}

void watch::init(std::array<Clock::time_point, num_points> &records) {
    for(auto& s : records) {
        s = Clock::time_point();
    }
}

void watch::initialize_worker(watch::worker_id worker) {
    if(records_.count(worker) == 0) {
        init(records_[worker]);
    }
}
} // namespace
