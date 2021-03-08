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

#include <jogasaki/data/value_store.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief aggregate group context
 */
class aggregate_group_context : public context_base {
public:
    friend class aggregate_group;
    /**
     * @brief create empty object
     */
    aggregate_group_context() = default;

    /**
     * @brief create new object
     */
    aggregate_group_context(
        class abstract::task_context* ctx,
        block_scope& variables,
        memory_resource* resource,
        memory_resource* varlen_resource,
        std::vector<data::value_store> stores,
        std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> resources,
        std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores,
        std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> nulls_resources
    );

    /**
     * @see context_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see context_base::release()
     */
    void release() override;

private:
    std::vector<data::value_store> stores_{};
    std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> resources_{};
    std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> nulls_resources_{};
    std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores_{};
};

}


