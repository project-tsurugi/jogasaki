/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <memory>
#include <vector>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/scan_range.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/transaction_context.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief scan context
 */
class scan_context : public context_base {
public:
    friend class scan;
    /**
     * @brief create empty object
     */
    scan_context() = default;

    /**
     * @brief create new object
     */
    scan_context(
        class abstract::task_context* ctx,
        variable_table& variables,
        std::unique_ptr<kvs::storage> stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        transaction_context* tx,
        impl::scan_range const* range,
        memory_resource* resource,
        memory_resource* varlen_resource,
        kvs::transaction* strand
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

    [[nodiscard]] transaction_context* transaction() const noexcept;

    /**
     * @brief Support for debugging, callable in GDB: ctx->dump()
     */
    void dump() const noexcept;

    [[nodiscard]] kvs::transaction* strand() const noexcept;
private:
    std::unique_ptr<kvs::storage> stg_{};
    std::unique_ptr<kvs::storage> secondary_stg_{};
    transaction_context* tx_{};
    std::unique_ptr<kvs::iterator> it_{};
    std::size_t yield_count_{};
    impl::scan_range const* range_{};
    kvs::transaction* strand_{};
};

} // namespace jogasaki::executor::process::impl::ops
