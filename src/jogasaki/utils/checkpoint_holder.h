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

#include <takatori/util/fail.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::utils {

using takatori::util::fail;

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

    explicit checkpoint_holder(memory_resource* resource) noexcept :
        resource_(resource),
        checkpoint_(resource_->get_checkpoint())
    {}

    ~checkpoint_holder() {
        resource_->deallocate_after(checkpoint_);
    }

private:
    memory_resource* resource_{};
    checkpoint checkpoint_{};
};

}
