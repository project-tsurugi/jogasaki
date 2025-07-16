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

#include <type_traits>

#include <jogasaki/data/iterable_record_store.h>

namespace jogasaki::utils {

using iterator = data::iterable_record_store::iterator;

template <class Iterator>
struct iterator_pair {
    iterator_pair(Iterator x, Iterator y) : first(x), second(y) {
        static_assert(std::is_trivially_copyable_v<Iterator>);
        static_assert(std::is_trivially_copyable_v<iterator_pair<Iterator>>);
    }

    iterator_pair() = default;
    ~iterator_pair() = default;
    iterator_pair(iterator_pair const& other) = default;
    iterator_pair& operator=(iterator_pair const& other) = default;
    iterator_pair(iterator_pair&& other) noexcept = default;
    iterator_pair& operator=(iterator_pair&& other) noexcept = default;

    Iterator first; //NOLINT
    Iterator second; //NOLINT
};

template <class Iterator>
bool empty(iterator_pair<Iterator> const& p) {
    return p.first == p.second;
}

}
