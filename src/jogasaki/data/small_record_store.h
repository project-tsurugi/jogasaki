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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/accessor/record_copier.h>
#include <jogasaki/utils/aligned_unique_ptr.h>

namespace jogasaki::data {

using takatori::util::maybe_shared_ptr;

/**
 * @brief records container to store just handful of records
 */
class small_record_store {
public:

    /// @brief type of record pointer
    using record_pointer = void*;

    /**
     * @brief create empty object
     */
    small_record_store() = default;

    /**
     * @brief create new instance
     * @param meta the record metadata
     * @param varlen_resource memory resource used to store the varlen data referenced from the records stored in this
     * instance. nullptr is allowed if this instance stores only the copy of reference to varlen data (shallow copy.)
     * @param capacity the capacity of the container
     */
    explicit small_record_store(
        maybe_shared_ptr<meta::record_meta> meta,
        memory::paged_memory_resource* varlen_resource = nullptr,
        std::size_t capacity = 1
    );

    /**
     * @brief copy and store the record
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object.
     * @param record source of the record added to this container
     * @param index the index for the record to be stored. Must be less than the capacity.
     * @return pointer to the stored record
     */
    record_pointer set(accessor::record_ref record, std::size_t index = 0);

    /**
     * @brief getter for the number of data count added to this store
     * @return the number of records
     */
    [[nodiscard]] std::size_t capacity() const noexcept;

    /**
     * @brief get accessor to N-th record
     * @param index the index for the record
     * @return the accessor to the record specified by the index
     */
    [[nodiscard]] accessor::record_ref ref(std::size_t index = 0) const noexcept;

    /**
     * @brief return whether the object is valid or not
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief compare contents of two objects
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(small_record_store const& a, small_record_store const& b) noexcept;
    friend bool operator!=(small_record_store const& a, small_record_store const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, small_record_store const& value);
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::size_t capacity_{};
    memory::paged_memory_resource* varlen_resource_{};
    accessor::record_copier copier_{};
    std::size_t record_size_{};
    utils::aligned_array<std::byte> data_ = utils::make_aligned_array<std::byte>(0UL, 0UL);
};

} // namespace
