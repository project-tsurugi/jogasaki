/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <jogasaki/executor/io/record_reader.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief take_flat context
 */
class take_flat_context : public context_base {
public:
    friend class take_flat;

    /**
     * @brief create empty object
     */
    take_flat_context() = default;

    /**
     * @brief create new object
     */
    take_flat_context(
        class abstract::task_context* ctx,
        variable_table& variables,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

private:
    io::record_reader* reader_{};
};

}


