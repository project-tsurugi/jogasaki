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
#include "work_context.h"

namespace jogasaki::executor::process::impl {

work_context::work_context(
    class request_context* request_context,
    std::size_t operator_count,
    std::size_t block_count,
    std::unique_ptr<memory_resource> resource, std::unique_ptr<memory_resource> varlen_resource,
    std::shared_ptr<kvs::database> database, std::shared_ptr<kvs::transaction> transaction) :
    request_context_(request_context),
    contexts_(operator_count),
    resource_(std::move(resource)),
    varlen_resource_(std::move(varlen_resource)),
    database_(std::move(database)),
    transaction_(std::move(transaction))
{
    variables_.reserve(block_count);
}

ops::context_container& work_context::contexts() noexcept {
    return contexts_;
}

work_context::block_scopes& work_context::scopes() noexcept {
    return variables_;
}

block_scope& work_context::variables(std::size_t block_index) noexcept {
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

kvs::transaction* work_context::transaction() const noexcept {
    return transaction_.get();
}

request_context* work_context::req_context() const noexcept {
    return request_context_;
}
}