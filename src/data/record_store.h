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

#include <memory/paged_memory_resource.h>
#include <accessor/record_copier.h>

namespace jogasaki::data {

/**
 * @brief auto-expanding container to store any number of fixed length records
 * @details This container can store any number of fragments with any length.
 * No iterator is provided for the stored data. References for each fragment needs to be kept and managed outside the container.
 */
class record_store {
public:
    using pointer = void*;

    record_store() = default;

    record_store(memory::paged_memory_resource* resource, std::shared_ptr<meta::record_meta> meta) :
            resource_(resource), meta_(std::move(meta)), copier_(meta_) {}

    pointer append(accessor::record_ref record) {
        auto sz = meta_->record_size();
        auto* p = resource_->allocate(meta_->record_size(), meta_->record_alignment());
        if (!p) std::abort();
        copier_.copy(record, p, sz);
        ++count_;
        return p;
    }

    /**
     * @brief getter for the number of data count added to this region
     * @return the number of data
     */
    [[nodiscard]] std::size_t count() const noexcept {
        return count_;
    }

    /**
     * @return whether the region is empty or not
     */
    [[nodiscard]] bool empty() const noexcept {
        return count_ == 0;
    }

private:
    memory::paged_memory_resource* resource_{};
    std::shared_ptr<meta::record_meta> meta_{};
    accessor::record_copier copier_{};
    std::size_t count_{};
};

} // namespace
