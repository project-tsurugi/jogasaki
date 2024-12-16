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

#include <optional>
#include <random>
#include <vector>

#include <jogasaki/dist/key_distribution.h>
#include <jogasaki/kvs/storage.h>

namespace jogasaki::dist {

/**
 * @brief key_distribution subclass assuming keys are distributed uniformly
 * @details This class assumes that keys are distributed uniformly and calculate the pivots based on this assumption.
 */
class uniform_key_distribution : public key_distribution {
public:

    using size_type  = key_distribution::size_type;
    using range_type = key_distribution::range_type;
    using pivot_type = key_distribution::pivot_type;

    constexpr uniform_key_distribution() = default;

    ~uniform_key_distribution() override = default;

    uniform_key_distribution(uniform_key_distribution const&) = default;
    uniform_key_distribution& operator=(uniform_key_distribution const&) = default;
    uniform_key_distribution(uniform_key_distribution&&) = default;
    uniform_key_distribution& operator=(uniform_key_distribution&&) = default;

    uniform_key_distribution(
        kvs::storage& stg,
        kvs::transaction& tx
    ) :
      stg_(std::addressof(stg)),
      tx_(std::addressof(tx))
    {}

    [[nodiscard]] std::optional<double> estimate_count(range_type const& range) override;

    [[nodiscard]] std::optional<double> estimate_key_size(range_type const& range) override;

    [[nodiscard]] std::optional<double> estimate_value_size(range_type const& range) override;

    [[nodiscard]] std::vector<pivot_type> compute_pivots(size_type max_count, range_type const& range) override;

    /**
     * @brief the smallest key in the index
     * @param out[out] the smallest key
     * @return status::ok if the operation is successful
     * @return status::not_found if the index is empty or failed to get the smallest key
     * @return otherwise, other status code
     * @note the function is public for testing
     */
    status lowkey(pivot_type& out);

    /**
     * @brief the largest key in the index
     * @param out[out] the largest key
     * @return status::ok if the operation is successful
     * @return status::not_found if the index is empty or failed to get the largest key
     * @return otherwise, other status code
     * @note the function is public for testing
     */
    status highkey(pivot_type& out);

private:
    kvs::storage* stg_{};
    kvs::transaction* tx_{};

    status scan_one(bool reverse, uniform_key_distribution::pivot_type& out);
};

double string_distance(std::string_view lo, std::string_view hi);

std::size_t common_prefix_len(std::string_view lo, std::string_view hi);

std::vector<std::string>
generate_strings(std::string_view lo, std::string_view hi, std::size_t chars = 256);

}  // namespace jogasaki::dist
