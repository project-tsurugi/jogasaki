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

#include <jogasaki/executor/process/step.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/iterator.h>
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
        variable_table& variables,
        std::unique_ptr<kvs::storage> stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        kvs::transaction* tx,
        impl::scan_info const* scan_info,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

    [[nodiscard]] kvs::transaction* transaction() const noexcept;

private:
    std::unique_ptr<kvs::storage> stg_{};
    std::unique_ptr<kvs::storage> secondary_stg_{};
    kvs::transaction* tx_{};
    std::unique_ptr<kvs::iterator> it_{};
    impl::scan_info const* scan_info_{};
};

}


