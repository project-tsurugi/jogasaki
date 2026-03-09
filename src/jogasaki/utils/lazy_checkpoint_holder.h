/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <jogasaki/utils/assert.h>

namespace jogasaki::utils {

/**
 * @brief checkpoint for lifo memory resource with explicit arm/release/reset semantics.
 * @details Unlike checkpoint_holder, this class does not arm the checkpoint at
 *          construction and does not free memory at destruction. The intended usage is:
 *          - Call set_checkpoint() once to arm before the loop.
 *          - Call release() at the top of each loop iteration to free allocations back
 *            to the checkpoint without disarming (checkpoint remains valid for the next
 *            iteration).
 *          - Call reset() when the loop finishes and the checkpoint is no longer needed;
 *            this performs release() and then unset(), returning the object to its
 *            freshly-constructed state.
 *          - On yield, call neither release() nor reset() — the checkpoint is preserved
 *            so that resume can continue from the right position.
 */
class lazy_checkpoint_holder {
public:
    using memory_resource = memory::lifo_paged_memory_resource;
    using checkpoint = typename memory_resource::checkpoint;

    lazy_checkpoint_holder(lazy_checkpoint_holder const& other) = delete;
    lazy_checkpoint_holder& operator=(lazy_checkpoint_holder const& other) = delete;
    lazy_checkpoint_holder(lazy_checkpoint_holder&& other) noexcept = delete;
    lazy_checkpoint_holder& operator=(lazy_checkpoint_holder&& other) noexcept = delete;

    /**
     * @brief create empty object with no associated resource.
     */
    lazy_checkpoint_holder() noexcept = default;

    /**
     * @brief create object associated with a resource without arming the checkpoint.
     * @details Call set_checkpoint() to arm when ready.
     * @param resource the memory resource managed by this object
     */
    explicit lazy_checkpoint_holder(memory_resource* resource) noexcept :
        resource_(resource)
    {}

    /**
     * @brief destructor — does not release memory.
     */
    ~lazy_checkpoint_holder() = default;

    /**
     * @brief save the current position as the checkpoint.
     * @details Must not be called when a checkpoint is already set.
     */
    void set_checkpoint() {
        assert_with_exception(! checkpoint_set_, checkpoint_set_);
        if (resource_ != nullptr) {
            checkpoint_ = resource_->get_checkpoint();
            checkpoint_set_ = true;
        }
    }

    /**
     * @brief deallocate memory back to the checkpoint without disarming it.
     * @details Calling release() multiple times frees to the same checkpoint each time
     *          (idempotent with respect to the memory position, checkpoint stays set).
     *          No-op if no checkpoint is set.
     */
    void release() {
        if (checkpoint_set_) {
            resource_->deallocate_after(checkpoint_);
        }
    }

    /**
     * @brief release memory back to the checkpoint and disarm the checkpoint.
     * @details Equivalent to release() followed by unset(). After this call the object
     *          is in the same state as after construction; set_checkpoint() may be called
     *          again. No-op if no checkpoint is set.
     */
    void reset() {
        release();
        checkpoint_set_ = false;
    }

    /**
     * @brief disarm the checkpoint without freeing any memory.
     * @details After this call, release() and reset() are no-ops until set_checkpoint()
     *          is called again.
     */
    void unset() noexcept {
        checkpoint_set_ = false;
    }

private:
    memory_resource* resource_{};
    checkpoint checkpoint_{};
    bool checkpoint_set_{false};
};

}  // namespace jogasaki::utils
