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

#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::utils {

/**
 * @brief RAII checkpoint for lifo memory resource.
 * @details Saves the current memory position at construction and deallocates
 *          all memory allocated after that point at destruction.
 */
class checkpoint_holder {
public:
    using memory_resource = memory::lifo_paged_memory_resource;
    using checkpoint = typename memory_resource::checkpoint;

    checkpoint_holder(checkpoint_holder const& other) = delete;
    checkpoint_holder& operator=(checkpoint_holder const& other) = delete;
    checkpoint_holder(checkpoint_holder&& other) noexcept = delete;
    checkpoint_holder& operator=(checkpoint_holder&& other) noexcept = delete;

    /**
     * @brief create empty object with no associated resource.
     */
    checkpoint_holder() noexcept = default;

    /**
     * @brief create new object and save the current checkpoint.
     * @param resource the memory resource managed by this object
     */
    explicit checkpoint_holder(memory_resource* resource) noexcept :
        resource_(resource),
        checkpoint_(resource_->get_checkpoint())
    {}

    /**
     * @brief destruct the object and deallocate memory back to the checkpoint.
     */
    ~checkpoint_holder() {
        reset();
    }

    /**
     * @brief deallocate memory back to the checkpoint.
     * @details This call is idempotent: calling it multiple times (or letting the
     *          destructor call it after an explicit call) is safe.
     */
    void reset() {
        if (resource_ != nullptr) {
            resource_->deallocate_after(checkpoint_);
        }
    }

private:
    memory_resource* resource_{};
    checkpoint checkpoint_{};
};

}  // namespace jogasaki::utils
