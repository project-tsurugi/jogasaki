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
#include "convert_offset.h"

namespace jogasaki::utils {

takatori::datetime::time_point remove_offset(time_point_tz tptz) {
    return tptz.first - std::chrono::minutes{tptz.second};
}

time_point_tz add_offset(takatori::datetime::time_point tp, std::int32_t offset_min) {
    tp += std::chrono::minutes{offset_min};
    return {tp, offset_min};
}

takatori::datetime::time_of_day remove_offset(time_of_day_tz todtz) {
    return todtz.first - std::chrono::minutes{todtz.second};
}

time_of_day_tz add_offset(takatori::datetime::time_of_day tod, std::int32_t offset_min) {
    tod += std::chrono::minutes{offset_min};
    return {tod, offset_min};
}

}  // namespace jogasaki::utils
