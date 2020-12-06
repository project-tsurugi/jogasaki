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
     * @param meta schema information for the records to be compared. This applies to both lhs/rhs records assuming their metadata are identical.
     * @attention record_meta is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    explicit comparator(meta::record_meta const* meta) noexcept : l_meta_(meta), r_meta_(meta) {}

    /**
     * @brief construct new object with separate metadata for lhs/rhs
     * @details metadata on lhs/rhs must be compatible, i.e. the field types and orders are identical, except the nullability and value/nullity offsets.
     * @param l_meta schema information for the lhs records to be compared
     * @param r_meta schema information for the rhs records to be compared
     * @attention record_meta are kept and used by the comparator. The caller must ensure it outlives this object.
     */
    comparator(
        meta::record_meta const* l_meta,
        meta::record_meta const* r_meta
    ) noexcept;

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
    meta::record_meta const* l_meta_{};
    meta::record_meta const* r_meta_{};

    [[nodiscard]] int compare_field(accessor::record_ref const& a, accessor::record_ref const& b, std::size_t field_index) const;
};

}
