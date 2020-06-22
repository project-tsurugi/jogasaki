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
    /// @brief type of record pointer
    using record_pointer = void*;

    /**
     * @brief create new instance
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
        data_(utils::make_aligned_array<char>(meta_->record_alignment(), meta_->record_size()*capacity_))
    {}

    /**
     * @brief copy and store the record
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object.
     * @param record source of the record added to this container
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
     * @brief reset store state except the state managed by memory resource
     * @details To keep consistency, caller needs to reset or release appropriately (e.g. deallocate to some check point)
     * the memory resources passed to constructor when calling this function.
     */
    void reset() noexcept {
        // no-op
    }

    accessor::record_ref ref(std::size_t index = 0) {
        return accessor::record_ref(data_.get()+meta_->record_size()*index, meta_->record_size());
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    std::size_t capacity_{};
    memory::paged_memory_resource* varlen_resource_{};
    utils::aligned_array<char> data_;
    accessor::record_copier copier_{};
};

} // namespace
