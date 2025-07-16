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

#include <cstring>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_copier.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/memory/fifo_paged_memory_resource.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::data {

using takatori::util::maybe_shared_ptr;

/**
 * @brief FIFO auto-expanding container to store any number of record
 * @details This container can store any number of records, which are backed by paged memory resource.
 * The stored data can be popped in FIFO order. Only one producer and one consumer can push/pop the data at a time.
 * This container support variable length data such as text field, whose non-SSO data are backed by another paged
 * memory resource.
 */
class cache_align fifo_record_store {
public:
    /// @brief type of record pointer
    using record_pointer = void*;

    using checkpoint = memory::fifo_paged_memory_resource::checkpoint;

    using queue_entry = std::pair<record_pointer, checkpoint>;

    /**
     * @brief create empty object
     */
    fifo_record_store() = default;

    /**
     * @brief create new instance
     * @param record_resource fifo memory resource used to store records
     * @param varlen_resource fifo memory resource used to store varlen data referenced from records.
     * nullptr is allowed if this instance stores only the copy of reference to varlen data (shallow copy.)
     * @param meta record metadata
     */
    fifo_record_store(
            memory::fifo_paged_memory_resource* record_resource,
            memory::fifo_paged_memory_resource* varlen_resource,
            maybe_shared_ptr<meta::record_meta> meta
    );

    /**
     * @brief push the record by copying fields data
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object unless it's nullptr.
     * @param record source of the record added to this container
     * @return pointer to the stored record
     */
    record_pointer push(accessor::record_ref record);

    /**
     * @brief try pop the record from the store
     * For varlen data such as text, the data exists on the varlen resource assigned to this object unless it's nullptr.
     * @param out filled with the record popped from the container. The record is accessible until the next pop.
     * @return true if the record is popped successfully, false if the container is empty
     */
    bool try_pop(accessor::record_ref& out);

    /**
     * @brief prepare record region at the end of the store and provider the pointer so that the record is filled by caller
     * @return pointer to the stored record
     */
    [[nodiscard]] record_pointer allocate_record();

    /**
     * @brief getter for the number of data count added to this store
     * @return the number of records
     */
    [[nodiscard]] std::size_t count() const noexcept;

    /**
     * @return whether the container is empty or not
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief reset store state including the state managed by memory resource
     * @details To keep consistency, caller needs to reset or release appropriately (e.g. deallocate to some check point)
     * the memory resources passed to constructor when calling this function.
     */
    void reset() noexcept;

    /**
     * @brief accessor to metadata
     * @return record meta held by this object
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    /**
     * @return variable length resources
     */
    [[nodiscard]] memory::paged_memory_resource* varlen_resource() const noexcept;

    /**
     * @return copier to copy data into this store
     */
    [[nodiscard]] accessor::record_copier & copier() noexcept;

private:
    memory::fifo_paged_memory_resource* resource_{};
    memory::fifo_paged_memory_resource* varlen_resource_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    accessor::record_copier copier_{};
    std::atomic_size_t count_{};
    std::size_t original_record_size_{};
    std::size_t positive_record_size_{};
    tbb::concurrent_queue<queue_entry> queue_{};
    record_pointer prev_{};
    checkpoint prev_cp_{};

};

} // namespace
