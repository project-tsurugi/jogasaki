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
#include "task_context.h"

#include <jogasaki/executor/process/result_store_writer.h>
#include <jogasaki/executor/process/data_channel_writer.h>

namespace jogasaki::executor::process::impl {

task_context::task_context(std::size_t partition) :
    partition_(partition)
{}

process::impl::task_context::task_context(
    request_context& rctx,
    std::size_t partition,
    io_exchange_map const& io_exchange_map,
    std::shared_ptr<impl::scan_info> scan_info,
    data::iterable_record_store* result,
    api::data_channel* channel
) :
    request_context_(std::addressof(rctx)),
    partition_(partition),
    io_exchange_map_(std::addressof(io_exchange_map)),
    scan_info_(std::move(scan_info)),
    result_(result),
    channel_(channel)
{}

reader_container task_context::reader(task_context::reader_index idx) {
    auto& flow = io_exchange_map_->input_at(idx)->data_flow_object(*request_context_);
    using step_kind = model::step_kind;
    switch(flow.kind()) {
        case step_kind::group:
            return unsafe_downcast<exchange::group::flow>(flow).sources()[partition_].acquire_reader(); //NOLINT
        case step_kind::aggregate:
            return unsafe_downcast<exchange::aggregate::flow>(flow).sources()[partition_].acquire_reader(); //NOLINT
        case step_kind::forward:
            return unsafe_downcast<exchange::forward::flow>(flow).sources()[partition_].acquire_reader(); //NOLINT
            //TODO other exchanges
        default:
            fail();
    }
    return {};
}

record_writer* task_context::downstream_writer(task_context::writer_index idx) {
    auto& flow = io_exchange_map_->output_at(idx)->data_flow_object(*request_context_);
    using step_kind = model::step_kind;
    switch(flow.kind()) {
        case step_kind::group:
            return &unsafe_downcast<exchange::group::flow>(flow).sinks()[partition_].acquire_writer(); //NOLINT
        case step_kind::aggregate:
            return &unsafe_downcast<exchange::aggregate::flow>(flow).sinks()[partition_].acquire_writer(); //NOLINT
        case step_kind::forward:
            return &unsafe_downcast<exchange::forward::flow>(flow).sinks()[partition_].acquire_writer(); //NOLINT
            //TODO other exchanges
        default:
            fail();
    }
    return {};
}

record_writer* task_context::external_writer() {
    BOOST_ASSERT(io_exchange_map_->external_output() != nullptr);  //NOLINT
    BOOST_ASSERT(result_ != nullptr || channel_ != nullptr);  //NOLINT
    auto& op = *unsafe_downcast<ops::emit>(io_exchange_map_->external_output());
    if (! external_writer_) {
        if (result_) {
            external_writer_ = std::make_shared<result_store_writer>(*result_, op.meta());
        } else {
            external_writer_ = std::make_shared<data_channel_writer>(*channel_, op.meta());
        }
    }
    return external_writer_.get();
}

class abstract::scan_info const* task_context::scan_info() {
    return scan_info_.get();
}

std::size_t task_context::partition() const noexcept {
    return partition_;
}

}
