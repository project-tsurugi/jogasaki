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

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor {

/**
 * @brief field ordering
 */
enum class ordering : std::size_t {
    //@brief default ordering (treated as ascending)
    undefined = 0,

    //@brief ascending ordering
    ascending,

    //@brief descending ordering
    descending,
};

/**
 * @brief information on record comparison
 */
class compare_info {
public:
    /**
     * @brief construct empty object
     */
    compare_info() = default;

    /**
     * @brief construct new object with separate metadata for compared lhs/rhs
     * @details metadata on lhs/rhs must be compatible, i.e. the field types and orders are identical, except the
     * nullability and value/nullity offsets.
     * @param left schema information for the lhs records to be compared
     * @param right schema information for the rhs records to be compared
     * @param orders the ordering of each field in the compared records. Pass empty vector if default ordering is used
     * for all fields. If not empty, it must contain ordering for all the fields in the compared record.
     * @attention the reference to left/right args are kept and used by the comparator. The caller must ensure they
     * outlives this object.
     */
    compare_info(
        meta::record_meta const& left,
        meta::record_meta const& right,
        std::vector<ordering> orders
    ) noexcept;

    /**
     * @brief construct new object with separate metadata for lhs/rhs
     * @details metadata on lhs/rhs must be compatible, i.e. the field types and orders are identical, except the nullability and value/nullity offsets.
     * @param l_meta schema information for the lhs records to be compared
     * @param r_meta schema information for the rhs records to be compared
     * @attention record_meta are kept and used by the comparator. The caller must ensure it outlives this object.
     */
    compare_info(
        meta::record_meta const& left,
        meta::record_meta const& right
    ) noexcept;

    /**
     * @brief construct new object
     * @param meta schema information for the records to be compared. This applies to both lhs/rhs records
     * assuming their metadata are identical.
     * @param orders the ordering of each field in the compared records. Pass empty vector if default ordering is used
     * for all fields. If not empty, it must contain ordering for all the fields in the compared record.
     * @attention the reference to meta arg is kept and used by the comparator. The caller must ensure it
     * outlives this object.
     */
    explicit compare_info(
        meta::record_meta const& meta,
        std::vector<ordering> orders = {}
    ) noexcept;

    /**
     * @brief accessor to the lhs record meta
     */
    [[nodiscard]] meta::record_meta const& left() const noexcept;

    /**
     * @brief accessor to the rhs record meta
     */
    [[nodiscard]] meta::record_meta const& right() const noexcept;

    /**
     * @brief return whether the comparison result should be opposite of the underlying compare result. Typically used
     * to tell if the descending ordering should be handled for the given field.
     * @param field_index the field index in the compared record
     * @return true if the field should be handled as descending
     * @return false otherwise
     */
    [[nodiscard]] bool opposite(std::size_t field_index) const noexcept;

private:
    meta::record_meta const* left_{};
    meta::record_meta const* right_{};
    std::vector<ordering> orders_{};
};

}
