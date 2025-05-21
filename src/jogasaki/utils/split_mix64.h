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

#include <memory>

namespace jogasaki::utils {

constexpr std::uint64_t split_mix64(std::uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x>>30U)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x>>27U)) * 0x94d049bb133111ebULL;
    return x ^ (x>>31U);
}

/**
 * @brief hash compare class to mix hash values using split mix 64
 */
class split_mix64_hash_compare {
public:

    [[nodiscard]] std::size_t hash(std::uint64_t const& a ) const {
        return split_mix64(a);
    }

    [[nodiscard]] bool equal(std::uint64_t const& a, std::uint64_t const& b) const {
        return a == b;
    }
};

}  // namespace jogasaki::utils
