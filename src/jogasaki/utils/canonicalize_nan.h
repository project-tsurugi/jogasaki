/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <cmath>
#include <limits>
#include <type_traits>

namespace jogasaki::utils {

/**
 * @brief canonicalize NaN so that all NaNs have the identical bit pattern
 * @details NaN produced by floating point calculation can have different bit patterns (e.g. sign bit is set for
 * Infinity - Infinity on x86, or by sign inversion). This function replaces any NaN with the canonical NaN
 * (i.e. `std::numeric_limits<T>::quiet_NaN()` - positive sign, quiet bit set and zero payload) as required by the
 * spec.
 * Values other than NaN, or values of non-floating point types are returned as they are.
 * @tparam T the type of the value
 * @param value the source value
 * @return the canonical NaN if `value` is NaN
 * @return `value` otherwise
 */
template <class T>
[[nodiscard]] constexpr T canonicalize_nan(T value) noexcept {
    if constexpr (std::is_floating_point_v<T>) {
        if(std::isnan(value)) {
            return std::numeric_limits<T>::quiet_NaN();
        }
    }
    return value;
}

}  // namespace jogasaki::utils
