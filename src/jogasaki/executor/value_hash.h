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

#include <cstddef>
#include <functional>

#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::executor {

/**
 * @brief hash function for a single field value
 * @details this is consistent with `executor::equal_to`, i.e. the values equal by `equal_to` have the same hash.
 * Use this instead of `std::hash` when the values are hashed for grouping, distinct, or partitioning.
 * @tparam T the runtime type of the field value
 */
template <class T>
struct value_hash {
    std::size_t operator()(T const& value) const noexcept {
        return std::hash<T>{}(value);
    }
};

template <>
struct value_hash<runtime_t<meta::field_type_kind::decimal>> {
    std::size_t operator()(runtime_t<meta::field_type_kind::decimal> const& value) const noexcept {
        // reduce so that numerically equal values (e.g. 1.0 and 1) have the same hash
        return std::hash<runtime_t<meta::field_type_kind::decimal>>{}(utils::reduce_triple(value));
    }
};

}  // namespace jogasaki::executor
