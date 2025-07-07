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
#include <vector>
#include <map>

namespace jogasaki::utils {

template<class T>
std::vector<T> subtract_vectors(std::vector<T> const& x, std::vector<T> const& y) {
    std::map<T, std::size_t> m{};
    for(auto&& e : x) {
        ++m[e];
    }
    for(auto&& e : y) {
        if(m.count(e) > 0) {
            --m[e];
        }
    }
    std::vector<T> ret{};
    ret.reserve(x.size());
    for(auto [k, count] : m) {
        for(std::size_t i = 0; i < count; ++i) {
            ret.emplace_back(k);
        }
    }
    return ret;
}

}

