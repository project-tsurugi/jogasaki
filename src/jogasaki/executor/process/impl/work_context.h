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

#include <memory>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/block_scope.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief processor working context implementation for production
 */
class work_context : public process::abstract::work_context {
public:
    using block_scopes = std::vector<block_scope>;
    using memory_resource = ops::context_base::memory_resource;

    /**
     * @brief create empty object
     */
    work_context() = default;

    /**
     * @brief create new object
     * @param operator_count the number of operators contained in the process
     * @param block_count the number of block scopes in the process
     * @param resource memory resource used as the process work area
     * @param database the kvs database shared within the request
     */
    work_context(
        std::size_t operator_count,
        std::size_t block_count,
        std::unique_ptr<memory_resource> resource,
        std::shared_ptr<kvs::database> database
    ) :
        contexts_(operator_count),
        resource_(std::move(resource)),
        database_(std::move(database))
    {
        variables_.reserve(block_count);
    }

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
    [[nodiscard]] ops::context_container& contexts() noexcept {
        return contexts_;
    }

    /**
     * @brief accessor to block scopes
     * @return the list of block scopes that the processor uses to store variables
     */
    [[nodiscard]] block_scopes& scopes() noexcept {
        return variables_;
    }

    /**
     * @brief accessor to a block scope
     * @param block_index the index to designate the block scope within the list
     * @return the block scope
     */
    [[nodiscard]] block_scope& variables(std::size_t block_index) noexcept {
        BOOST_ASSERT(block_index < variables_.size());  //NOLINT
        return variables_[block_index];
    }

    /**
     * @brief accessor to memory resource
     * @return the memory resource that processor can use as work area
     */
    [[nodiscard]] memory_resource* resource() const noexcept {
        return resource_.get();
    }

    /**
     * @brief accessor to kvs database
     * @return the database that is shared within request
     */
    [[nodiscard]] kvs::database* database() const noexcept {
        return database_.get();
    }
private:
    ops::context_container contexts_{};
    block_scopes variables_{};
    std::unique_ptr<memory_resource> resource_{};
    std::shared_ptr<kvs::database> database_{};
};

}


