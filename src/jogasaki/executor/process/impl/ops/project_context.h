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

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief project context
 */
class project_context : public context_base {
public:
    friend class project;
    /**
     * @brief create empty object
     */
    project_context() = default;

    /**
     * @brief create new object
     */
    project_context(
        class abstract::task_context* ctx,
        block_scope& variables,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

};

}


