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

#include <limits>
#include <cstdint>

namespace jogasaki::scan_cli {

constexpr static std::size_t time_point_begin = 0;
constexpr static std::size_t time_point_storage_prepared = 1;
constexpr static std::size_t time_point_start_preparing_output_buffer = 2;
constexpr static std::size_t time_point_output_buffer_prepared = 3;
constexpr static std::size_t time_point_start_creating_request = 4;
constexpr static std::size_t time_point_request_created = 5;
constexpr static std::size_t time_point_schedule = 6;
constexpr static std::size_t time_point_schedule_completed = 7;
constexpr static std::size_t time_point_result_dumped = 8;
constexpr static std::size_t time_point_close_db = 9;
constexpr static std::size_t time_point_release_pool = 10;
constexpr static std::size_t time_point_start_completion = 11;
constexpr static std::size_t time_point_end_completion = 12;

}
