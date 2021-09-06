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

#include <vector>
#include <memory>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/storage.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief full write operator context
 */
class write_full_context : public context_base {
public:
    friend class write_full;
    /**
     * @brief create empty object
     */
    write_full_context() = default;

    /**
     * @brief create new object
     */
    write_full_context(
        class abstract::task_context* ctx,
        variable_table& input_variables,
        std::unique_ptr<kvs::storage> stg,
        kvs::transaction* tx,
        sequence::manager* sequence_manager,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

    /**
     * @brief accessor to transaction held by this object
     */
    [[nodiscard]] kvs::transaction* transaction() const noexcept;

private:
    std::unique_ptr<kvs::storage> stg_{};
    kvs::transaction* tx_{};
    sequence::manager* sequence_manager_{};
    data::aligned_buffer key_buf_{};
    data::aligned_buffer value_buf_{};
};


}


