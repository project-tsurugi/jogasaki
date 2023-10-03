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
#include "context_helper.h"

#include <takatori/util/downcast.h>
#include <jogasaki/kvs/database.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

context_helper::context_helper(
    abstract::task_context &context
) noexcept :
    context_(std::addressof(context)),
    work_context_(unsafe_downcast<work_context>(context_->work_context()))
{}

variable_table& context_helper::variable_table(std::size_t index) {
    return work_context_->variables(index); //NOLINT
}

context_container& context_helper::contexts() const noexcept {
    return work_context_->contexts();
}

context_helper::memory_resource* context_helper::resource() const noexcept {
    return work_context_->resource();
}

context_helper::memory_resource* context_helper::varlen_resource() const noexcept {
    return work_context_->varlen_resource();
}

kvs::database* context_helper::database() const noexcept {
    return work_context_->database();
}

transaction_context* context_helper::transaction() const noexcept {
    return work_context_->transaction();
}

abstract::task_context* context_helper::task_context() const noexcept {
    return context_;
}

request_context* context_helper::req_context() const noexcept {
    if(! work_context_) {
        return nullptr;
    }
    return work_context_->req_context();
}

bool context_helper::empty_input_from_shuffle() const noexcept {
    return work_context_->empty_input_from_shuffle();
}

}


