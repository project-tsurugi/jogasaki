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

#include <limits>
#include <cstdint>

namespace jogasaki::join_cli {

constexpr static std::size_t time_point_prepare = 0;
constexpr static std::size_t time_point_produce = 1;
constexpr static std::size_t time_point_produced = 2;
constexpr static std::size_t time_point_consume = 3;
constexpr static std::size_t time_point_consumed = 4;
constexpr static std::size_t time_point_release_pool = 5;
constexpr static std::size_t time_point_start_completion = 6;
constexpr static std::size_t time_point_end_completion = 7;

}
