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
#include <functional>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include "compare_info.h"

namespace jogasaki::executor {

/**
 * @brief record comparator
 */
class comparator {
public:
    /**
     * @brief construct empty object
     */
    comparator() = default;

    /**
     * @brief construct new object
     * @param info comparison information and metadata for the records to be compared.
     * @attention info is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    explicit comparator(compare_info const& info) noexcept;

    /**
     * @brief compare function
     * @param a 1st argument for comparison
     * @param b 2nd argument for comparison
     * @return negative if a < b
     * @return positive if a > b
     * @return zero if a is equivalent to b
     */
    [[nodiscard]] int operator()(accessor::record_ref const& a, accessor::record_ref const& b) const noexcept;

private:
    compare_info const* meta_{};

    [[nodiscard]] int compare_field(
        accessor::record_ref const& a,
        accessor::record_ref const& b,
        std::size_t field_index
    ) const;
    [[nodiscard]] int negate_if(int ret, std::size_t field_index) const noexcept;
};

}
