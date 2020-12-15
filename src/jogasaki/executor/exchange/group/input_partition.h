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

#include <jogasaki/request_context.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/shuffle/pointer_table.h>

namespace jogasaki::executor::exchange::group {

/**
 * @brief partitioned input data handled in upper phase in shuffle
 * @details This object represents group exchange input data after partition.
 * This object is transferred between sinks and sources when transfer is instructed to the exchange.
 * No limit to the number of records stored in this object.
 * After populating input data (by write() and flush()), this object provides iterators to the internal pointer tables
 * (each of which needs to fit page size defined by memory allocator, e.g. 2MB for huge page)
 * which contain sorted pointers.
 */
class cache_align input_partition {
public:
    using pointer_table_type = shuffle::pointer_table;
    using pointer_tables_type = std::vector<pointer_table_type>;
    using iterator = pointer_tables_type::iterator;
    using table_iterator = pointer_table_type::iterator;
    constexpr static std::size_t ptr_table_size = memory::page_size/sizeof(void*);

    input_partition() = default;

    /**
     * @brief create new instance
     * @param resource
     * @param info
     */
    input_partition(
        std::unique_ptr<memory::paged_memory_resource> resource_for_records,
        std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables,
        std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data,
        std::shared_ptr<group_info> info,
        request_context* context,
        [[maybe_unused]] std::size_t pointer_table_size = ptr_table_size
    );

    /**
     * @brief write record to the input partition
     * @param record
     * @return whether flushing pointer table happens or not
     */
    bool write(accessor::record_ref record);

    /**
     * @brief finish current pointer table
     * @details the current internal pointer table is finalized and next write() will create one.
     */
    void flush();

    /**
     * @brief beginning iterator for pointer tables
     */
    [[nodiscard]] iterator begin();

    /**
     * @brief ending iterator for pointer tables
     */
    [[nodiscard]] iterator end();

    /**
     * @brief returns the number of pointer tables
     */
    [[nodiscard]] std::size_t tables_count() const noexcept;

private:
    std::unique_ptr<memory::paged_memory_resource> resource_for_records_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data_{};
    std::shared_ptr<group_info> info_{};
    request_context* context_{};
    std::unique_ptr<data::record_store> records_{};
    pointer_tables_type pointer_tables_{};
    comparator comparator_{};
    bool current_pointer_table_active_{false};
    std::size_t max_pointers_{};

    void initialize_lazy();
};

}
