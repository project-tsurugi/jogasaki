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
#include <jogasaki/executor/process/step.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/iterator.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief scan context
 */
class write_partial_context : public context_base {
public:
    friend class write_partial;
    /**
     * @brief create empty object
     */
    write_partial_context() = default;

    /**
     * @brief create new object
     */
    write_partial_context(
        class abstract::task_context* ctx,
        block_scope& variables,
        std::unique_ptr<kvs::storage> stg,
        kvs::transaction* tx,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta,
        memory_resource* resource,
        memory_resource* varlen_resource
    ) :
        context_base(ctx, variables, resource, varlen_resource),
        stg_(std::move(stg)),
        tx_(tx),
        key_store_(std::move(key_meta)),
        value_store_(std::move(value_meta))
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::write_partial;
    }

    void release() override {

    }

    [[nodiscard]] kvs::transaction* transaction() const noexcept {
        return tx_;
    }

private:
    std::unique_ptr<kvs::storage> stg_{};
    kvs::transaction* tx_{};
    data::aligned_buffer key_buf_{};
    data::aligned_buffer value_buf_{};
    data::small_record_store key_store_{};
    data::small_record_store value_store_{};
};

}


