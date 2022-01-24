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

    [[nodiscard]] operator kvs::transaction&() const noexcept {  //NOLINT
        return *transaction_;
    }

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

    /**
     * @brief commit the transaction
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status commit(bool async = false) {
        return transaction_->commit(async);
    }

    /**
     * @brief wait for commit
     * @details wait for async commit
     * @return status::ok if the operation is successful
     * @return status::err_time_out if waiting timed out
     * @return other status code when error occurs
     */
    [[nodiscard]] status wait_for_commit(std::size_t timeout_ns = 0UL) {
        return transaction_->wait_for_commit(timeout_ns);
    }

    /**
     * @brief abort the transaction
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status abort() {
        return transaction_->abort();
    }

    /**
     * @brief return the native transaction control handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::TransactionControlHandle control_handle() const noexcept {
        return transaction_->control_handle();
    }

    /**
     * @brief return the native handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::TransactionHandle handle() noexcept {
        return transaction_->handle();
    }

    /**
     * @brief return the parent database object
     * @return the parent database
     */
    [[nodiscard]] kvs::database* database() const noexcept {
        return transaction_->database();
    }
private:
    std::shared_ptr<kvs::transaction> transaction_{};
    std::size_t id_{};

    static inline std::atomic_size_t id_source_{};
};

inline std::shared_ptr<transaction_context> wrap(std::unique_ptr<kvs::transaction> arg) noexcept {
    return std::make_shared<transaction_context>(std::shared_ptr<kvs::transaction>{std::move(arg)});
}

}

