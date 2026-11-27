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

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variables_view.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/transaction_context.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief write_create operator context
 */
class write_create_context : public context_base {
public:
    friend class write_create;
    /**
     * @brief create empty object
     */
    write_create_context() = default;

    /**
     * @brief create new object
     */
    write_create_context(
        class abstract::task_context* ctx,
        variables_view variables,
        transaction_context* tx,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

    [[nodiscard]] transaction_context* transaction() const noexcept;

private:
    transaction_context* tx_{};
};

}  // namespace jogasaki::executor::process::impl::ops
