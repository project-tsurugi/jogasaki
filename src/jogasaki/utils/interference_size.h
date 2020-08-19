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

#include <cstddef>

namespace jogasaki::utils {

static constexpr std::size_t hardware_destructive_interference_size = 64; // replace with std one when C++17 becomes available

} // namespace

#ifdef ENABLE_CACHE_ALIGN
#define OPTIONAL_CACHE_ALIGN alignas(jogasaki::utils::hardware_destructive_interference_size)  //NOLINT clang-tidy confuses with constant
#else
#define OPTIONAL_CACHE_ALIGN
#endif
