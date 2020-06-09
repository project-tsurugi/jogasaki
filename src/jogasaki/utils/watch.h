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

#include <mutex>
#include <unordered_map>
#include <chrono>
#include <memory>
#include <vector>

namespace jogasaki::utils {

class watch {
public:
    using Clock = std::chrono::steady_clock;
    using Duration = std::chrono::milliseconds;

    /**
     * @brief identifier to distinguish workers
     */
    using worker_id = std::size_t;

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

    watch() noexcept;

    void restart();

    bool set_point(point_in_code loc, worker_id worker = -1);

    Clock::time_point base();

    /**
     * @brief retrieve the time when first comes at the point of code
     */
    Clock::time_point view_first(point_in_code loc);

    /**
     * @brief retrieve the time when last leaves the point of code
     */
    Clock::time_point view_last(point_in_code loc);

    /**
     * @brief calculate duration between two time point
     * @param begin time point defining beginning of the interval
     * @param end time point defining end of the interval
     * @param complementary if false, interval begins when first thread comes and ends when last leaves
     * if true, interval begins when last thread comes and ends when first leaves
     * @return duration
     */
    std::size_t duration(point_in_code begin, point_in_code end, bool complementary = false);

    std::size_t average_duration(point_in_code begin, point_in_code end);

    std::unique_ptr<std::vector<std::size_t>> durations(point_in_code begin, point_in_code end);

private:
    std::chrono::time_point<Clock> begin_{};
    std::unordered_map<worker_id, std::array<Clock::time_point, num_points>> records_{};
    std::mutex guard_{};

    void init(std::array<Clock::time_point, num_points>& records);
    void initialize_worker(worker_id worker);
};

watch& get_watch();

} // namespace
