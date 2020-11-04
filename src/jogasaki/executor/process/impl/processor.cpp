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

#include <jogasaki/executor/process/step.h>
#include "block_scope_info.h"
#include "ops/operator_builder.h"

namespace jogasaki::executor::process::impl {

processor::processor(
    std::shared_ptr<processor_info> info,
    plan::compiler_context const& compiler_ctx,
    std::shared_ptr<io_info> io_info,
    std::shared_ptr<relation_io_map> relation_io_map,
    std::shared_ptr<kvs::database> database
) :
    info_(std::move(info)),
    operators_(ops::create_operators(info_, compiler_ctx, std::move(io_info), std::move(relation_io_map))),
    relation_io_map_(std::move(relation_io_map)),
    database_(std::move(database))
{}

abstract::status processor::run(abstract::task_context *context) {
    // initialize work_context
    auto* work = static_cast<work_context*>(context->work_context()); //NOLINT
    for(auto& block_info : info_->scopes_info()) {
        work->scopes().emplace_back(block_info);
    }
    ops::operator_executor visitor{
        const_cast<relation::graph_type&>(info_->relations()),
        info_->compiled_info(),
        &operators_,
        context,
        work->resource(),
        database_.get()
    };
    visitor();
    // TODO handling status code
    return abstract::status::completed;
}

}
