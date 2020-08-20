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

#include <takatori/util/object_creator.h>

#include <array>
#include <vector>
#include <cstring>

#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/accessor/record_copier.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::exchange::group {

/**
 * @brief auto-expanding container to store any number of record
 * @details This container can store any number of records, which are backed by paged memory resource.
 * No iterator is provided for the stored data. Reference for each record needs to be kept and managed outside the container.
 * This container support variable length data such as text field, whose non-SSO data are backed by another paged
 * memory resource.
 * Resources referenced from this object (e.g. head_) are owned and managed by backing paged_memory_resource, so
 * this object doesn't clean up or release them on destruction. Their lifetime is defined by the backing memory resource.
 */
class cache_align pointer_table {
public:
    using pointer = void*;

    using iterator = pointer*;

    /**
     * @brief create empty object
     */
    pointer_table() = default;

    /**
     * @brief create new instance
     * @param varlen_resource memory resource used to store varlen data referenced from records
     * @param meta record metadata
     */
    pointer_table(memory::paged_memory_resource* resource, std::size_t capacity) : resource_(resource), capacity_(capacity) {
        head_ = static_cast<iterator>(resource_->allocate(sizeof(pointer)*capacity_, alignof(pointer)));
    }

    /**
     * @brief copy and store the record
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object.
     * @param record source of the record added to this container
     * @return pointer to the stored record
     */
    pointer emplace_back(pointer p) {
        if (size_ < capacity_) {
            head_[size_] = p;
            ++size_;
        }
        return p;
    }

    /**
     * @brief getter for the number of data count added to this store
     * @return the number of records
     */
    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return capacity_;
    }
    /**
     * @return whether the region is empty or not
     */
    [[nodiscard]] bool empty() const noexcept {
        return size_ == 0;
    }

    [[nodiscard]] iterator begin() const noexcept {
        return head_;
    }

    [[nodiscard]] iterator end() const noexcept {
        return head_+size_;
    }

private:
    memory::paged_memory_resource* resource_{};
    iterator head_{};
    std::size_t size_{0};
    std::size_t capacity_{0};
};

} // namespace
