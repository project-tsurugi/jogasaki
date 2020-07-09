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
#include <jogasaki/storage/storage_context.h>
#include <jogasaki/storage/transaction_context.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::relop {

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
    explicit scan_context(
        std::shared_ptr<storage::storage_context> storage
    ) :
        storage_(std::move(storage))
    {}

    operator_kind kind() override {
        return operator_kind::scan;
    }
private:
    std::shared_ptr<storage::storage_context> storage_{};
    std::shared_ptr<storage::transaction_context> tx_{};
};

}


