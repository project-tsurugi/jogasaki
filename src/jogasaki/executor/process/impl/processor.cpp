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
#include "processor.h"
#include "block_variables_info.h"
#include "relop/relational_operators_builder.h"

namespace jogasaki::executor::process::impl {

processor::processor(std::shared_ptr<processor_info> info) :
    info_(std::move(info))
{
    auto ops = relop::create_relational_operators(info_);
    ops.set_block_index(info_->blocks_index());
    operators_ = std::move(ops);
}

abstract::status processor::run(abstract::task_context *context) {
    // initialize work_context
    auto* work = static_cast<work_context*>(context->work_context()); //NOLINT
    for(auto& blk : variables_info_) {
        work->variables().emplace_back(blk);
    }
    relop::operators_executor visitor{
        const_cast<graph::graph<relation::expression>&>(info_->operators()),
        info_->compiled_info(),
        &operators_,
        context
    };
    visitor.process(); //TODO loop
    return abstract::status::completed;
}

}
