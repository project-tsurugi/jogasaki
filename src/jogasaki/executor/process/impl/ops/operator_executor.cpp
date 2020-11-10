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
#include "operator_executor.h"

#include <takatori/util/downcast.h>
#include <jogasaki/kvs/database.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

operator_executor::operator_executor(
    abstract::task_context &context
) noexcept :
    context_(std::addressof(context)),
    work_context_(unsafe_downcast<work_context>(context_->work_context()))
{}

block_scope& operator_executor::block_scope(std::size_t index) {
    return work_context_->variables(index); //NOLINT
}

context_container& operator_executor::contexts() const noexcept {
    return work_context_->contexts();
}

operator_executor::memory_resource* operator_executor::resource() const noexcept {
    return work_context_->resource();
}

kvs::database* operator_executor::database() const noexcept {
    return work_context_->database();
}

abstract::task_context* operator_executor::task_context() const noexcept {
    return context_;
}

}


