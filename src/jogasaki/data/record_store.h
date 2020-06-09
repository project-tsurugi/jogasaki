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

#include <vector>
#include <cstring>

#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/accessor/record_copier.h>

namespace jogasaki::data {

/**
 * @brief auto-expanding container to store any number of record
 * @details This container can store any number of records, which are backed by paged memory resource.
 * No iterator is provided for the stored data. Reference for each record needs to be kept and managed outside the container.
 * This container support variable length data such as text field, whose non-SSO data are backed by another paged
 * memory resource.
 */
class record_store {
public:
    /// @brief type of record pointer
    using record_pointer = void*;

    /**
     * @brief create empty object
     */
    record_store() = default;

    /**
     * @brief create new instance
     * @param record_resource memory resource used to store records
     * @param varlen_resource memory resource used to store varlen data referenced from records
     * @param meta record metadata
     */
    record_store(
            memory::paged_memory_resource* record_resource,
            memory::paged_memory_resource* varlen_resource,
            std::shared_ptr<meta::record_meta> meta) :
            resource_(record_resource), meta_(std::move(meta)), copier_(meta_, varlen_resource)
    {}

    /**
     * @brief copy and store the record
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object.
     * @param record source of the record added to this container
     * @return pointer to the stored record
     */
    record_pointer append(accessor::record_ref record);

    /**
     * @brief getter for the number of data count added to this store
     * @return the number of records
     */
    [[nodiscard]] std::size_t count() const noexcept;

    /**
     * @return whether the region is empty or not
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief reset store state except the state managed by memory resource
     * @details To keep consistency, caller needs to reset or release appropriately (e.g. deallocate to some check point)
     * the memory resources passed to constructor when calling this function.
     */
    void reset() noexcept;

private:
    memory::paged_memory_resource* resource_{};
    std::shared_ptr<meta::record_meta> meta_{};
    accessor::record_copier copier_{};
    std::size_t count_{};
};

} // namespace
