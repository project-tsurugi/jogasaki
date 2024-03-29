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

constexpr static std::size_t time_point_prepare = 0;
constexpr static std::size_t time_point_prepared = 1;
constexpr static std::size_t time_point_touched = 2;
constexpr static std::size_t time_point_produce = 3;
constexpr static std::size_t time_point_produced = 4;

constexpr static std::size_t sync_wait_prepare = 100;
