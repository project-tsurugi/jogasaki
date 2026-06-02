/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/process/impl/region_id.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief a lightweight view into the process variable tables.
 * @details Provides read and write access to the stream variables held in a
 *     variable_table_list.  The view is identified by a block index that
 *     designates the "current" block; variables belonging to ancestor blocks
 *     are resolved transparently through the variable_table_info parent chain.
 *
 *     A default-constructed view is empty (null container). exists() returns
 *     false for an empty view, and ref() must not be called on one.
 */
class variables_view {
public:
    using variable = takatori::descriptor::variable;

    /**
     * @brief create empty (null) view
     */
    variables_view() = default;

    /**
     * @brief create a view for the given region in the list
     * @param list the list of variable_tables for all regions in the process
     * @param block_index index of the current basic block (region)
     */
    variables_view(variable_table_list& list, std::size_t block_index) noexcept :
        list_(std::addressof(list)),
        block_index_(block_index)
    {}

    /**
     * @brief check whether a variable is accessible through this view
     * @details Returns false when the view is empty (null container).
     * @param var the variable descriptor to check
     * @return true if the variable exists in the current block or any ancestor
     */
    [[nodiscard]] bool exists(variable const& var) const noexcept {
        if (list_ == nullptr) {
            return false;
        }
        return (*list_)[block_index_].info().exists(var);
    }

    /**
     * @brief retrieve value_info for a variable
     * @details Delegates to the current block's variable_table_info which
     *     handles ancestor block delegation transparently.
     * @param var the variable descriptor
     * @return value_info containing offset and block_index for the variable
     * @throws std::out_of_range if the variable is not found
     */
    [[nodiscard]] value_info const& at(variable const& var) const {
        return (*list_)[block_index_].info().at(var);
    }

    /**
     * @brief get record_ref for the specified region's variable store
     * @details If r is undefined, returns the current region's ref.
     * @param r region_id designating the region to read, or undefined for the current region
     * @return record_ref for the designated region
     */
    [[nodiscard]] accessor::record_ref ref(region_id r = region_id{}) const {
        auto idx = r ? r.index() : block_index_;
        return (*list_)[idx].store().ref();
    }

    /**
     * @brief return the current block index
     */
    [[nodiscard]] std::size_t block_index() const noexcept {
        return block_index_;
    }

    /**
     * @brief return a pointer to the current block's variable_table, or nullptr if out of range.
     * @details intended for debug/dump use only.
     */
    [[nodiscard]] variable_table const* current_table() const noexcept {
        if (list_ == nullptr || block_index_ >= list_->size()) {
            return nullptr;
        }
        return std::addressof((*list_)[block_index_]);
    }

    /**
     * @brief return whether the view refers to a valid (non-null) variable table list
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return list_ != nullptr;
    }

private:
    variable_table_list* list_{};
    std::size_t block_index_{};
};

}  // namespace jogasaki::executor::process::impl
