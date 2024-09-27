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
#pragma once

#include "key_range.h"

namespace jogasaki::dist {

/**
 * @brief provides the key distribution information on index.
 */
class key_distribution {
  public:
    using size_type  = std::size_t;
    using range_type = key_range;
    using pivot_type = key_range::key_type;

    constexpr key_distribution() = default;

    virtual ~key_distribution() = default;
    /**
     * @brief computes the estimated count of the entries in the range on index.
     * @param range the range on index
     * @return the estimated count of the entries in the range
     * @return empty if it is not available
     */
    [[nodiscard]] virtual std::optional<double> estimate_count(range_type const& range) = 0;
    /**
     * @brief computes the estimated key size in the range on index.
     * @param range the range on index
     * @return the estimated key size of an entry in the range
     * @return empty if it is not available
     */
    [[nodiscard]] virtual std::optional<double> estimate_key_size(range_type const& range) = 0;
    /**
     * @brief computes the estimated value size in the range on index.
     * @param range the range on index
     * @return the estimated value size of an entry in the range
     * @return empty if it is not available
     */
    [[nodiscard]] virtual std::optional<double> estimate_value_size(range_type const& range) = 0;
    /**
     * @brief compute a sequence of pivots that split the range on index.
     * @details
     *    The resulting pivot sequence is sorted by the order of the keys on the index,
     *    and does not include the keys at both begin and end of the range.
     * @param max_count maximum count of the pivots
     * @param range the range on index
     * @return a sequence of pivots, must be <= max_count
     * @return empty list if the range is not splittable
     * @note the returned pivots may not be the actual keys on the index,
     * and ill-formed from the actual keys
     * @post individual pivots are within the range
     */
    [[nodiscard]] virtual std::vector<pivot_type> compute_pivots(
        size_type max_count, range_type const& range) = 0;
};

} // namespace jogasaki::dist
