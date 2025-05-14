/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <cstddef>
#include <memory>
#include <vector>

#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/request_context.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief processor working context implementation for production
 */
class work_context : public process::abstract::work_context {
public:
    using variable_table_list = std::vector<variable_table>;
    using memory_resource = ops::context_base::memory_resource;

    /**
     * @brief create empty object
     */
    work_context() = default;

    /**
     * @brief create new object
     * @param request_context the request context for the task containing this context
     * @param operator_count the number of operators contained in the process
     * @param block_count the number of block in the process
     * @param resource memory resource used as the process work area
     * @param varlen_resource varlen memory resource used as the process work area
     * @param database the kvs database shared within the request
     * @param transaction the transaction used for the kvs access
     */
    work_context(
        request_context* request_context,
        std::size_t operator_count,
        std::size_t block_count,
        std::unique_ptr<memory_resource> resource,
        std::unique_ptr<memory_resource> varlen_resource,
        std::shared_ptr<kvs::database> database,
        std::shared_ptr<transaction_context> transaction,
        bool empty_input_from_shuffle,
        bool in_transaction_and_non_sticky
    );

    /**
     * @brief destruct the object
     */
    ~work_context() override = default;

    work_context(work_context const& other) = delete;
    work_context& operator=(work_context const& other) = delete;
    work_context(work_context&& other) noexcept = delete;
    work_context& operator=(work_context&& other) noexcept = delete;

    /**
     * @brief accessor to context container
     * @return the container for the contexts used by operators in the process
     */
    [[nodiscard]] ops::context_container& contexts() noexcept;

    /**
     * @brief accessor to block variables list
     * @return the list of block variable_tables that the processor uses to store variables
     */
    [[nodiscard]] variable_table_list& variable_tables() noexcept;

    /**
     * @brief accessor to variable table for the block
     * @param block_index the index to designate the block within the list
     * @return the variable table for the block
     */
    [[nodiscard]] variable_table& variables(std::size_t block_index) noexcept;

    /**
     * @brief accessor to memory resource
     * @return the memory resource that processor can use as work area
     */
    [[nodiscard]] memory_resource* resource() const noexcept;

    /**
     * @brief accessor to varlen memory resource
     * @return the memory resource that processor can use to work on varlen data
     */
    [[nodiscard]] memory_resource* varlen_resource() const noexcept;

    /**
     * @brief accessor to kvs database
     * @return the database that is shared within request
     */
    [[nodiscard]] kvs::database* database() const noexcept;

    /**
     * @brief accessor to kvs transaction
     * @return the transaction that is shared within request
     */
    [[nodiscard]] transaction_context* transaction() const noexcept;

    /**
     * @brief accessor to strand object
     * @return the strand object
     * @return nullptr if the no strand is assigned to task
     */
    [[nodiscard]] kvs::transaction* strand() const noexcept;

    /**
     * @brief setter of kvs strand
     */
    void strand(std::unique_ptr<kvs::transaction> arg) noexcept;

    /**
     * @brief dispose the strand object held by this object
     */
    void reset_strand() noexcept;

    /**
     * @brief accessor to request context
     * @return the request context that is shared within request
     */
    [[nodiscard]] request_context* req_context() const noexcept;

    /**
     * @brief accessor to the empty_input_from_shuffle flag
     * @return true if inputs are shuffle exchanges and all of them are empty
     * @return false otherwise
     */
    [[nodiscard]] bool empty_input_from_shuffle() const noexcept;

    /**
     * @brief accessor to the in_transaction_and_non_sticky flag
     * @return true if the task runs in-transaction and is not sticky
     * @return false otherwise
     */
    [[nodiscard]] bool in_transaction_and_non_sticky() const noexcept;

private:
    request_context* request_context_{};
    ops::context_container contexts_{};
    variable_table_list variables_{};
    std::unique_ptr<memory_resource> resource_{};
    std::unique_ptr<memory_resource> varlen_resource_{};
    std::shared_ptr<kvs::database> database_{};
    std::shared_ptr<transaction_context> transaction_{};
    bool empty_input_from_shuffle_{};
    bool in_transaction_and_non_sticky_{};
    std::unique_ptr<kvs::transaction> strand_{};
};

}


