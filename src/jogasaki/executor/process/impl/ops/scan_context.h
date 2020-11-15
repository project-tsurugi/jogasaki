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

#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/scan_info.h>
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
        block_scope& variables,
        std::unique_ptr<kvs::storage> stg,
        kvs::transaction* tx,
        impl::scan_info const* scan_info,
        memory_resource* resource = nullptr
    ) : context_base(ctx, variables, resource),
        stg_(std::move(stg)),
        tx_(tx),
        scan_info_(scan_info)
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::scan;
    }

    void release() override {
        if(it_) {
            // TODO revisit the life-time of storage objects
            it_ = nullptr;
        }
    }

    [[nodiscard]] kvs::transaction* transaction() const noexcept {
        return tx_;
    }

private:
    std::unique_ptr<kvs::storage> stg_{};
    kvs::transaction* tx_{};
    std::unique_ptr<kvs::iterator> it_{};
    impl::scan_info const* scan_info_{};
};

}


