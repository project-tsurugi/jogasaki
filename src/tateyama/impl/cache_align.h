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

#include <iomanip>
#include <locale>
#include <ostream>
#include <sstream>

// replace with std one when C++17 becomes available
static constexpr std::size_t hardware_destructive_interference_size = 64;

/**
 * @brief non conditional shorter qualifier to align on cache lines
 * @details To avoid false sharing, objects should be cache aligned if
 * 1. the objects are created(allocated) on one thread and accessed from different threads.
 * 2. the objects are mutable and changes are made frequently.
 */
#define cache_align alignas(hardware_destructive_interference_size)  //NOLINT

