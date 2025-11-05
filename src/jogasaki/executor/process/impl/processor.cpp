/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <utility>
#include <vector>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/ops/operator_container.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>

#include "variable_table_info.h"

namespace jogasaki::executor::process::impl {

using takatori::util::unsafe_downcast;

processor::processor(
    std::shared_ptr<processor_info> info,
    std::shared_ptr<ops::io_info> io_info,
    std::shared_ptr<relation_io_map> relation_io_map,
    io_exchange_map& io_exchange_map,
    request_context* request_context
) :
    info_(std::move(info)),
    operators_(
        ops::create_operators(
            info_,
            std::move(io_info),
            std::move(relation_io_map),
            io_exchange_map,
            request_context
        )
    ),
    relation_io_map_(std::move(relation_io_map))
{}

abstract::status processor::run(abstract::task_context *context) {
    // initialize work_context
    auto* work = unsafe_downcast<work_context>(context->work_context()); //NOLINT
    if (work->variable_tables().empty()) {
        for(auto& block_info : info_->vars_info_list()) {
            work->variable_tables().emplace_back(block_info);
        }
        // initialize req. stats to zero for INSERT/UPDATE/DELETE statements
        if(info_->details().has_write_operations()) {
            if(work->req_context()) {
                counter_kind kind = counter_kind::undefined;
                switch(info_->details().get_write_kind()) {
                    case write_kind::update: kind = counter_kind::updated; break;
                    case write_kind::delete_: kind = counter_kind::deleted; break;
                    case write_kind::insert: kind = counter_kind::inserted; break;
                    case write_kind::insert_overwrite: kind = counter_kind::merged; break;
                    case write_kind::insert_skip: kind = counter_kind::inserted; break;
                }
                work->req_context()->stats()->counter(kind).count(0);
            }
        }
    }
    auto status = unsafe_downcast<ops::record_operator>(operators_.root()).process_record(context);
    switch(status.kind()) {
        case ops::operation_status_kind::ok:
        case ops::operation_status_kind::aborted:
            return abstract::status::completed;
        case ops::operation_status_kind::yield:
            return abstract::status::to_yield;
        default:
            return abstract::status::completed;
    }
}

ops::operator_container const& processor::operators() const noexcept {
    return operators_;
}

std::shared_ptr<processor_info> const& processor::info() const noexcept {
    return info_;
}

} // namespace jogasaki::executor::process::impl
