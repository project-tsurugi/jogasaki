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

#include <cstddef>
#include <memory>
#include <chrono>

#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>

namespace jogasaki::utils {

using time_of_day_tz = std::pair<takatori::datetime::time_of_day, std::int32_t>;
using time_point_tz = std::pair<takatori::datetime::time_point, std::int32_t>;

/**
 * @brief convert time point with offset to time point without offset (UTC)
 * @param tptz time point with offset
 * @return time point without offset (UTC)
 */
takatori::datetime::time_point remove_offset(time_point_tz tptz);

/**
 * @brief convert time point without offset (UTC) to time point with offset
 * @param tp time point without offset
 * @param offset_min offset in minutes
 * @return time point with offset
 */
time_point_tz add_offset(takatori::datetime::time_point tp, std::int32_t offset_min);

/**
 * @brief convert time of day with offset to time of day without offset (UTC)
 * @param todtz time of day with offset
 * @return time of day without offset (UTC)
 */
takatori::datetime::time_of_day remove_offset(time_of_day_tz todtz);

/**
 * @brief convert time of day without offset (UTC) to time of day with offset
 * @param tod time of day without offset
 * @param offset_min offset in minutes
 * @return time of day with offset
 */
time_of_day_tz add_offset(takatori::datetime::time_of_day tod, std::int32_t offset_min);

}  // namespace jogasaki::utils
