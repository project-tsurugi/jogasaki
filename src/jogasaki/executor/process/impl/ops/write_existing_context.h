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

#include <memory>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/index/primary_context.h>
#include <jogasaki/index/secondary_context.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/transaction_context.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

/**
 * @brief write_existing operator context
 */
class write_existing_context : public context_base {
public:
    friend class write_existing;
    /**
     * @brief create empty object
     */
    write_existing_context() = default;

    /**
     * @brief create new object
     */
    write_existing_context(
        class abstract::task_context* ctx,
        variable_table& variables,
        std::unique_ptr<kvs::storage> stg,
        transaction_context* tx,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta,
        memory_resource* resource,
        memory_resource* varlen_resource,
        std::vector<index::secondary_context> secondary_contexts
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

    [[nodiscard]] transaction_context* transaction() const noexcept;

    [[nodiscard]] index::primary_context& primary_context() noexcept;

private:
    transaction_context* tx_{};
    index::primary_context primary_context_{};
    std::vector<index::secondary_context> secondary_contexts_{};
};

}  // namespace jogasaki::executor::process::impl::ops
