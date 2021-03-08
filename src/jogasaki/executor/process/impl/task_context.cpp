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

namespace jogasaki::executor::process::impl {

task_context::task_context(std::size_t partition) :
    partition_(partition)
{}

process::impl::task_context::task_context(
    std::size_t partition,
    io_exchange_map const& io_exchange_map,
    std::shared_ptr<impl::scan_info> scan_info,
    data::iterable_record_store* result
) :
    partition_(partition),
    io_exchange_map_(std::addressof(io_exchange_map)),
    scan_info_(std::move(scan_info)),
    result_(result),
    external_writers_(io_exchange_map_->external_output_count())
{}

reader_container task_context::reader(task_context::reader_index idx) {
    auto& flow = io_exchange_map_->input_at(idx)->data_flow_object();
    using step_kind = common::step_kind;
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
    auto& flow = io_exchange_map_->output_at(idx)->data_flow_object();
    using step_kind = common::step_kind;
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

record_writer* task_context::external_writer(task_context::writer_index idx) {
    BOOST_ASSERT(idx < external_writers_.size());  //NOLINT
    BOOST_ASSERT(result_ != nullptr);  //NOLINT
    auto& op = unsafe_downcast<ops::emit>(io_exchange_map_->external_output_at(idx));
    auto& slot = external_writers_.operator[](idx);
    if (! slot) {
        slot = std::make_shared<class external_writer>(*result_, op.meta());
    }
    return slot.get();
}

class abstract::scan_info const* task_context::scan_info() {
    return scan_info_.get();
}

std::size_t task_context::partition() const noexcept {
    return partition_;
}

}
