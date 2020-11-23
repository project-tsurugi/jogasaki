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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include "block_scope_info.h"
#include "ops/operator_builder.h"

namespace jogasaki::executor::process::impl {

using takatori::util::unsafe_downcast;

processor::processor(
    std::shared_ptr<processor_info> info,
    plan::compiler_context const& compiler_ctx,
    std::shared_ptr<io_info> io_info,
    std::shared_ptr<relation_io_map> relation_io_map,
    io_exchange_map& io_exchange_map,
    memory::lifo_paged_memory_resource* resource
) :
    info_(std::move(info)),
    operators_(ops::create_operators(info_, compiler_ctx, std::move(io_info), std::move(relation_io_map), io_exchange_map, resource)),
    relation_io_map_(std::move(relation_io_map))
{}

abstract::status processor::run(abstract::task_context *context) {
    // initialize work_context
    auto* work = static_cast<work_context*>(context->work_context()); //NOLINT
    for(auto& block_info : info_->scopes_info()) {
        work->scopes().emplace_back(block_info);
    }
    unsafe_downcast<ops::record_operator>(operators_.root()).process_record(context);
    // TODO handling status code
    return abstract::status::completed;
}

}
