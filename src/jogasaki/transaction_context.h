/*
 * Copyright 2018-2022 tsurugi project.
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

#include <atomic>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>

namespace jogasaki {

using takatori::util::maybe_shared_ptr;

/**
 * @brief context object for the transaction scope
 * @details this class represents context information in the scope of the transaction
 */
class transaction_context {
public:
    transaction_context() = default;

    /**
     * @brief create new context object
     * @param transaction the kvs transaction used in this context
     * @param result store to hold the result records, nullptr is allowed if the request doesn't create result set
     * @param data_channel data channel to write the data
     */
    explicit transaction_context(
        std::shared_ptr<kvs::transaction> transaction
    ) :
        transaction_(std::move(transaction)),
        id_(id_source_++)
    {}

    /**
     * @brief accessor for the wrapped transaction
     * @return transaction object in the kvs layer
     */
    [[nodiscard]] std::shared_ptr<kvs::transaction> const& object() const {
        return transaction_;
    }

    [[nodiscard]] std::size_t id() const noexcept {
        return id_;
    }

    [[nodiscard]] operator bool() const noexcept {
        return transaction_ != nullptr;
    }
private:
    std::shared_ptr<kvs::transaction> transaction_{};
    std::size_t id_{};

    static inline std::atomic_size_t id_source_{};
};

}

