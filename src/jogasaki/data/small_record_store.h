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
#include <jogasaki/utils/aligned_unique_ptr.h>

namespace jogasaki::data {

/**
 * @brief records container to store just handful of records
 */
class small_record_store {
public:
    static constexpr std::size_t hardware_destructive_interference_size = 64; // replace with std one when C++17 becomes available

    /// @brief type of record pointer
    using record_pointer = void*;

    /**
     * @brief create empty object
     */
    small_record_store() = default;

    /**
     * @brief create new instance
     * @param meta the record metadata
     * @param capacity the capacity of the container
     * @param varlen_resource memory resource used to store the varlen data referenced from the records stored in this
     * instance. nullptr is allowed if this instance stores only the copy of reference to varlen data (shallow copy.)
     */
    explicit small_record_store(
        std::shared_ptr<meta::record_meta> meta,
        std::size_t capacity = 1,
        memory::paged_memory_resource* varlen_resource = nullptr
    ) :
        meta_(std::move(meta)),
        capacity_(capacity),
        varlen_resource_(varlen_resource),
        copier_(meta_, varlen_resource_),
        data_(utils::make_aligned_array<std::byte>(
            std::max(meta_->record_alignment(), hardware_destructive_interference_size),
            meta_->record_size()*capacity_))
    {}

    /**
     * @brief copy and store the record
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object.
     * @param record source of the record added to this container
     * @param index the index for the record to be stored. Must be less than the capacity.
     * @return pointer to the stored record
     */
    record_pointer set(accessor::record_ref record, std::size_t index = 0) {
        auto sz = meta_->record_size();
        auto* p = ref(index).data();
        if (!p) std::abort();
        copier_(p, sz, record);
        return p;
    }

    /**
     * @brief getter for the number of data count added to this store
     * @return the number of records
     */
    [[nodiscard]] std::size_t capacity() const noexcept {
        return capacity_;
    }

    /**
     * @brief get accessor to N-th record
     * @param index the index for the record
     * @return the accessor to the record specified by the index
     */
    [[nodiscard]] accessor::record_ref ref(std::size_t index = 0) const noexcept {
        return accessor::record_ref(data_.get()+meta_->record_size()*index, meta_->record_size());
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    std::size_t capacity_{};
    memory::paged_memory_resource* varlen_resource_{};
    accessor::record_copier copier_{};
    utils::aligned_array<std::byte> data_ = utils::make_aligned_array<std::byte>(0UL, 0UL);
};

} // namespace
