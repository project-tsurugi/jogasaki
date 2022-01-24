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

#include <atomic>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/model/flow_repository.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>

namespace jogasaki {

using takatori::util::maybe_shared_ptr;

namespace scheduler {
class dag_controller;
class statement_scheduler;
class task_scheduler;
}

/**
 * @brief context object for the request scope
 * @details this class represents context information in the scope of the execution request
 */
class request_context {
public:
    /**
     * @brief create default context object
     */
    request_context();

    /**
     * @brief create new context object
     * @param ch channel used to communicate with scheduler
     * @param config global configuration
     * @param request_resource the memory resource used to construct request wide objects such as
     * processors and operators
     * @param database the kvs database shared within the request. Pass nullptr if the request doesn't access kvs.
     * @param result store to hold the result records, nullptr is allowed if the request doesn't create result set
     * @param data_channel data channel to write the data
     */
    explicit request_context(
        std::shared_ptr<class configuration> config,
        std::shared_ptr<memory::lifo_paged_memory_resource> request_resource = {},
        std::shared_ptr<kvs::database> database = {},
        std::shared_ptr<transaction_context> transaction = {},
        executor::sequence::manager* sequence_manager = {},
        data::result_store* result = {},
        maybe_shared_ptr<api::data_channel> data_channel = {}
    );

    /**
     * @brief accessor for the gloabl configuration
     * @return global configuration
     */
    [[nodiscard]] std::shared_ptr<class configuration> const& configuration() const;

    /**
     * @brief accessor for the result store
     * @return result store
     */
    [[nodiscard]] data::result_store* result();

    /**
     * @brief accessor for the request resource
     * @return request wide memory resource
     */
    [[nodiscard]] memory::lifo_paged_memory_resource* request_resource() const noexcept;

    /**
     * @brief accessor for the database
     * @return database shared within this request
     */
    [[nodiscard]] std::shared_ptr<kvs::database> const& database() const;

    /**
     * @brief accessor for the transaction
     * @return transaction shared within this request
     */
    [[nodiscard]] std::shared_ptr<transaction_context> const& transaction() const;

    /**
     * @brief accessor for the sequence manager
     * @return sequence manager shared within this request
     */
    [[nodiscard]] executor::sequence::manager* sequence_manager() const noexcept;

    /**
     * @brief setter for the result status
     * @details this overwrites the status code with more severe status code (e.g. warning is overwritten by an error)
     * If error code is already set to this object, this is no-op.
     * @return true if the give status is set
     * @return false if the error status is already set and nothing is changed
     * @note this function is thread-safe and multiple threads can call this function concurrently
     */
    bool status_code(status val) noexcept;

    /**
     * @brief accessor for the result status
     * @note this function is not thread-safe and should not be called concurrently with status_code(status val) above.
     */
    [[nodiscard]] status status_code() const noexcept;

    /**
     * @brief setter for the result message
     * @details this overwrites the existing status message. Use with setter status_code() and set the message only when
     * it updated the status correctly.
     * @note this function is thread-safe and multiple threads can call this function concurrently
     */
    void status_message(std::string_view val) noexcept;

    /**
     * @brief accessor for the result status message
     * @note this function is thread-safe and multiple threads can call this function concurrently
     */
    [[nodiscard]] std::string_view status_message() const noexcept;

    /**
     * @brief setter for the job context
     */
    void job(maybe_shared_ptr<scheduler::job_context> arg) noexcept;

    /**
     * @brief accessor for the job context
     * @return job context
     */
    [[nodiscard]] maybe_shared_ptr<scheduler::job_context> const& job() const noexcept;

    /**
     * @brief accessor for the data channel
     * @return data channel
     */
    [[nodiscard]] maybe_shared_ptr<api::data_channel> const& data_channel() const noexcept;

    /**
     * @brief setter for the flow repository
     */
    void flows(maybe_shared_ptr<model::flow_repository> arg) noexcept;

    /**
     * @brief accessor for the flow repository
     * @return flow repository
     */
    [[nodiscard]] maybe_shared_ptr<model::flow_repository> const& flows() const noexcept;

    /**
     * @brief setter for the task scheduler
     */
    void scheduler(maybe_shared_ptr<scheduler::task_scheduler> arg) noexcept;

    /**
     * @brief accessor for the task scheduler
     * @return task scheduler used in this request scope
     */
    [[nodiscard]] maybe_shared_ptr<scheduler::task_scheduler> const& scheduler() const noexcept;

    /**
     * @brief setter for the statement scheduler
     */
    void stmt_scheduler(maybe_shared_ptr<scheduler::statement_scheduler> arg) noexcept;

    /**
     * @brief accessor for the statement scheduler
     * @return statement scheduler
     */
    [[nodiscard]] maybe_shared_ptr<scheduler::statement_scheduler> const& stmt_scheduler() const noexcept;

private:
    std::shared_ptr<class configuration> config_{std::make_shared<class configuration>()};
    std::shared_ptr<memory::lifo_paged_memory_resource> request_resource_{};
    std::shared_ptr<kvs::database> database_{};
    std::shared_ptr<transaction_context> transaction_{};
    executor::sequence::manager* sequence_manager_{};
    data::result_store* result_{};
    maybe_shared_ptr<api::data_channel> data_channel_{};
    std::atomic<status> status_code_{status::ok};
    maybe_shared_ptr<scheduler::job_context> job_context_{};
    mutable std::mutex status_message_mutex_{};
    std::string status_message_{};
    maybe_shared_ptr<model::flow_repository> flows_{};
    maybe_shared_ptr<scheduler::task_scheduler> scheduler_{};
    maybe_shared_ptr<scheduler::statement_scheduler> statement_scheduler_{};
};

/**
 * @brief utility to set default task/statement schedulers (mainly for testing)
 * @param rctx the request context
 */
void prepare_scheduler(request_context& rctx);

}

