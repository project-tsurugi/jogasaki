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

namespace jogasaki::data {

/**
 * @brief auto-expanding container to store variable length data fragments
 * @details This container can store any number of fragments with any length.
 * No iterator is provided for the stored data. References for each fragment needs to be kept and managed outside the container.
 */
class variable_length_buffer {
public:
    using pointer = void*;

    variable_length_buffer(memory::paged_memory_resource* resource, std::size_t alignment) :
            resource_(resource), alignment_(alignment) {}

    pointer append(std::string_view sv) {
        return append(sv.data(), sv.size());
    }

    pointer append(void const* ptr, size_t size) {
        auto* p = resource_->allocate(size, alignment_);
        if (!p) std::abort();
        std::memcpy(p, ptr, size);
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
    std::size_t count_{};
    std::size_t alignment_{};
};

} // namespace
