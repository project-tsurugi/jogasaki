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
#include <jogasaki/constants.h>

namespace jogasaki::utils {

[[nodiscard]] constexpr bool is_power_of_two(std::size_t value) noexcept {
    return value != 0 && (value & (value - 1)) == 0;
}

[[nodiscard]] constexpr std::size_t round_down_to_power_of_two(std::size_t v) noexcept {
    if(is_power_of_two(v)) {
        return v;
    }

    if(v == 0) {
        return 0;
    }

    --v;
    for(std::size_t i = 1; i < sizeof(std::size_t) * bits_per_byte; i *= 2) {
        v |= v >> i;
    }

    return static_cast<std::size_t>(v + 1) >> 1U;
}

[[nodiscard]] constexpr std::size_t round_up_to_power_of_two(std::size_t v) noexcept {
    if(is_power_of_two(v)) {
        return v;
    }
    return round_down_to_power_of_two(v)*2;
}

}

