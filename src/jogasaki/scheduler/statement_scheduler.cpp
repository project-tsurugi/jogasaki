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
#include "statement_scheduler.h"

#include <takatori/util/downcast.h>
#include "statement_scheduler_impl.h"

namespace jogasaki::scheduler {

statement_scheduler::statement_scheduler() : statement_scheduler(std::make_shared<configuration>()) {}
statement_scheduler::statement_scheduler(std::shared_ptr<configuration> cfg, task_scheduler& scheduler) :
    impl_(std::make_unique<impl>(std::move(cfg), scheduler))
{}

statement_scheduler::statement_scheduler(std::shared_ptr<configuration> cfg) :
    impl_(std::make_unique<impl>(std::move(cfg)))
{};

statement_scheduler::statement_scheduler(maybe_shared_ptr<dag_controller> controller) noexcept:
    impl_(std::make_unique<impl>(std::move(controller)))
{}

statement_scheduler::~statement_scheduler() = default;

void statement_scheduler::schedule(
    model::statement const& s,
    request_context& context
) {
    return impl_->schedule(s, context);
}

} // namespace
