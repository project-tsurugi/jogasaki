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

#include <takatori/util/fail.h>
#include <takatori/util/downcast.h>

#include <jogasaki/utils/interference_size.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>

namespace jogasaki::scheduler {

using takatori::util::fail;
using takatori::util::unsafe_downcast;

class cache_align statement_scheduler::impl {
public:
    explicit impl(std::shared_ptr<configuration> cfg) :
        dag_controller_(cfg),
        cfg_(std::move(cfg))
    {}

    void schedule(
        model::statement& s,
        request_context& context
    ) {
        using kind = model::statement_kind;
        switch(s.kind()) {
            case kind::execute: {
                auto& g = unsafe_downcast<executor::common::execute>(s).operators();
                dag_controller_.schedule(g);
                break;
            }
            case kind::write: {
                auto& w = unsafe_downcast<executor::common::write>(s);
                w(context);
                break;
            }
        }
    }
private:
    dag_controller dag_controller_{};
    std::shared_ptr<configuration> cfg_{};
};

statement_scheduler::statement_scheduler() : statement_scheduler(std::make_shared<configuration>()) {}
statement_scheduler::statement_scheduler(std::shared_ptr<configuration> cfg) : impl_(std::make_unique<impl>(std::move(cfg))) {};
statement_scheduler::~statement_scheduler() = default;

void statement_scheduler::schedule(
    model::statement& s,
    request_context& context
) {
    return impl_->schedule(s, context);
}

} // namespace
