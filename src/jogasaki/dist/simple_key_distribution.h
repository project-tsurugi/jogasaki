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

#include "key_distribution.h"
#include <optional>
#include <random>
#include <vector>

namespace jogasaki::dist {

class simple_key_distribution : public key_distribution {
  public:
    using size_type  = key_distribution::size_type;
    using range_type = key_distribution::range_type;
    using pivot_type = key_distribution::pivot_type;

    constexpr simple_key_distribution() = default;
    ~simple_key_distribution() override = default;

    [[nodiscard]] std::optional<double> estimate_count(range_type const& range) override;
    [[nodiscard]] std::optional<double> estimate_key_size(range_type const& range) override;
    [[nodiscard]] std::optional<double> estimate_value_size(range_type const& range) override;
    [[nodiscard]] std::vector<pivot_type> compute_pivots(
        size_type max_count, range_type const& range) override;
};

} // namespace jogasaki::dist
