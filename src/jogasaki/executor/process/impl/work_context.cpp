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
#include "work_context.h"

#include <utility>
#include <boost/assert.hpp>

#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::process::impl {

work_context::work_context(
    class request_context* request_context,
    std::size_t operator_count,
    std::size_t block_count,
    std::unique_ptr<memory_resource> resource,
    std::unique_ptr<memory_resource> varlen_resource,
    std::shared_ptr<kvs::database> database,
    std::shared_ptr<transaction_context> transaction,
    bool empty_input_from_shuffle
) :
    request_context_(request_context),
    contexts_(operator_count),
    resource_(std::move(resource)),
    varlen_resource_(std::move(varlen_resource)),
    database_(std::move(database)),
    transaction_(std::move(transaction)),
    empty_input_from_shuffle_(empty_input_from_shuffle)
{
    variables_.reserve(block_count);
}

ops::context_container& work_context::contexts() noexcept {
    return contexts_;
}

work_context::variable_table_list& work_context::variable_tables() noexcept {
    return variables_;
}

variable_table& work_context::variables(std::size_t block_index) noexcept {
    BOOST_ASSERT(block_index < variables_.size());  //NOLINT
    return variables_[block_index];
}

work_context::memory_resource* work_context::resource() const noexcept {
    return resource_.get();
}

work_context::memory_resource* work_context::varlen_resource() const noexcept {
    return varlen_resource_.get();
}

kvs::database* work_context::database() const noexcept {
    return database_.get();
}

transaction_context* work_context::transaction() const noexcept {
    return transaction_.get();
}

request_context* work_context::req_context() const noexcept {
    return request_context_;
}

bool work_context::empty_input_from_shuffle() const noexcept {
    return empty_input_from_shuffle_;
}
}
