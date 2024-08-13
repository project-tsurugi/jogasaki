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
#include <memory>
#include <vector>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/fifo_record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/exchange/forward/forward_info.h>
#include <jogasaki/executor/exchange/shuffle/pointer_table.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::exchange::forward {

/**
 * @brief partitioned input data passed to forward operator
 * @details This object represents forward exchange input data per partition.
 * This object is shared by the sink and source that are assigned to the partition of the exchange.
 * No limit to the number of records stored in this object.
 */
class cache_align input_partition {
public:
    /**
     * @brief create empty object
     */
    input_partition() = default;

    /**
     * @brief create new instance
     * @param resource_for_records the memory resource to store records
     * @param resource_for_ptr_tables the memory resource backing pointer tables
     * @param resource_for_varlen_data the memory resource storing varlen data
     * @param info the forward information
     * @param context the request context
     * @param pointer_table_size the number of pointers that the pointer table can store (convenient for testing)
     */
    input_partition(
        std::unique_ptr<memory::fifo_paged_memory_resource> resource_for_records,
        std::unique_ptr<memory::fifo_paged_memory_resource> resource_for_ptr_tables,
        std::unique_ptr<memory::fifo_paged_memory_resource> resource_for_varlen_data,
        std::shared_ptr<forward_info> info,
        request_context* context
    );

    /**
     * @brief write record to the input partition
     * @param record the record reference to write
     * @return whether flushing pointer table happens or not
     */
    bool write(accessor::record_ref record);

    /**
     * @brief finish current pointer table
     * @details the current internal pointer table is finalized and next write() will create one.
     */
    void flush();

private:

    std::unique_ptr<memory::fifo_paged_memory_resource> resource_for_records_{};
    std::unique_ptr<memory::fifo_paged_memory_resource> resource_for_ptr_tables_{};
    std::unique_ptr<memory::fifo_paged_memory_resource> resource_for_varlen_data_{};
    std::shared_ptr<forward_info> info_{};
    request_context* context_{};
    std::unique_ptr<data::fifo_record_store> records_{};

    void initialize_lazy();
};

}  // namespace jogasaki::executor::exchange::forward
