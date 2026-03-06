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
#include <jogasaki/utils/fail.h>

namespace jogasaki::utils {

/**
 * @brief create check point for lifo memory resource and release on deconstruction
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
     *        All operations are no-ops until the object is replaced via assignment.
     */
    checkpoint_holder() noexcept = default;

    /**
     * @brief create new object
     * @param resource the memory resource managed by this object
     * @param defer if true, the checkpoint is not saved at construction time.
     *              Call set_checkpoint() later to save the checkpoint explicitly.
     */
    explicit checkpoint_holder(memory_resource* resource, bool defer = false) noexcept :
        resource_(resource),
        checkpoint_set_(! defer)
    {
        if (! defer) {
            checkpoint_ = resource_->get_checkpoint();
        }
    }

    /**
     * @brief destruct the object and deallocate the memory resource
     */
    ~checkpoint_holder() {
        reset();
    }

    /**
     * @brief save the current checkpoint of the memory resource.
     *        Can be called at any time after construction when defer=true was specified.
     *        If a checkpoint is already set, this call is ignored.
     */
    void set_checkpoint() {
        if (resource_ != nullptr && ! checkpoint_set_) {
            checkpoint_ = resource_->get_checkpoint();
            checkpoint_set_ = true;
        }
    }

    /**
     * @brief deallocate the memory resource to the point where the checkpoint was set.
     *        If no checkpoint is set, this is a no-op.
     */
    void reset() {
        if (checkpoint_set_) {
            resource_->deallocate_after(checkpoint_);
            checkpoint_set_ = false;
        }
    }

private:
    memory_resource* resource_{};
    checkpoint checkpoint_{};
    bool checkpoint_set_{false};
};

}  // namespace jogasaki::utils
