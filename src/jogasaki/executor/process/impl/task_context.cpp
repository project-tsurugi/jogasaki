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
#include "task_context.h"

#include <utility>
#include <boost/assert.hpp>

#include <takatori/util/reference_list_view.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/process/impl/ops/details/encode_key.h>
#include <jogasaki/executor/exchange/aggregate/flow.h>
#include <jogasaki/executor/exchange/forward/flow.h>
#include <jogasaki/executor/exchange/group/flow.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/exchange/source.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/model/flow.h>
#include <jogasaki/model/step_kind.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::process::impl {

task_context::task_context(std::size_t partition) :
    partition_(partition)
{}

process::impl::task_context::task_context(
    request_context& rctx,
    std::size_t partition,
    io_exchange_map const& io_exchange_map,
    std::shared_ptr<impl::scan_info> scan_info,
    io::record_channel* channel,
    partition_index sink_index
) :
    request_context_(std::addressof(rctx)),
    partition_(partition),
    io_exchange_map_(std::addressof(io_exchange_map)),
    scan_info_(std::move(scan_info)),
    channel_(channel),
    sink_index_(sink_index)
{}

io::reader_container task_context::reader(task_context::reader_index idx) {
    auto& flow = io_exchange_map_->input_at(idx)->data_flow_object(*request_context_);
    using step_kind = model::step_kind;
    VLOG_LP(log_trace) << "requested reader from exchange flow(" << &flow << ") partition_:" << partition_;
    switch(flow.kind()) {
        case step_kind::group:
            return unsafe_downcast<exchange::group::flow>(flow).sources()[partition_].acquire_reader(); //NOLINT
        case step_kind::aggregate:
            return unsafe_downcast<exchange::aggregate::flow>(flow).sources()[partition_].acquire_reader(); //NOLINT
        case step_kind::forward:
            return unsafe_downcast<exchange::forward::flow>(flow).sources()[partition_].acquire_reader(); //NOLINT
            //TODO other exchanges
        default:
            fail_with_exception();
    }
    std::abort();
}

void task_context::deactivate_writer(writer_index idx) {
    auto& flow = io_exchange_map_->output_at(idx)->data_flow_object(*request_context_);
    using step_kind = model::step_kind;
    switch(flow.kind()) {
        case step_kind::group:
            return unsafe_downcast<exchange::group::flow>(flow).sinks()[sink_index_].deactivate(); //NOLINT
        case step_kind::aggregate:
            return unsafe_downcast<exchange::aggregate::flow>(flow).sinks()[sink_index_].deactivate(); //NOLINT
        case step_kind::forward:
            return unsafe_downcast<exchange::forward::flow>(flow).sinks()[sink_index_].deactivate(); //NOLINT
        default:
            fail_with_exception();
    }
    std::abort();
}

io::record_writer* task_context::downstream_writer(task_context::writer_index idx) {
    auto& flow = io_exchange_map_->output_at(idx)->data_flow_object(*request_context_);
    using step_kind = model::step_kind;
    switch(flow.kind()) {
        case step_kind::group:
            return &unsafe_downcast<exchange::group::flow>(flow).sinks()[sink_index_].acquire_writer(); //NOLINT
        case step_kind::aggregate:
            return &unsafe_downcast<exchange::aggregate::flow>(flow).sinks()[sink_index_].acquire_writer(); //NOLINT
        case step_kind::forward:
            return &unsafe_downcast<exchange::forward::flow>(flow).sinks()[sink_index_].acquire_writer(); //NOLINT
        default:
            fail_with_exception();
    }
    std::abort();
}

io::record_writer* task_context::external_writer() {
    BOOST_ASSERT(channel_ != nullptr);  //NOLINT
    if (! external_writer_) {
        if(auto res = channel_->acquire(external_writer_); res != status::ok) {
            fail_with_exception();
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

io::record_channel* task_context::channel() const noexcept {
    return channel_;
}

impl::work_context& task_context::getImplWorkContext() const {
    return *dynamic_cast<impl::work_context*>(work_context());
}

void task_context::encode_key(){
    std::size_t blen{};
    std::string msg{};
    executor::process::impl::variable_table vars{};
    if(auto res = impl::ops::details::encode_key(
        request_context_,
        scan_info_->begin_columns(),
        vars,
        *getImplWorkContext().varlen_resource(),
        scan_info_->key_begin(),
        blen,
        msg
      );
       res != status::ok) {
        if(res == status::err_type_mismatch) {
            // only on err_type_mismatch, msg is filled with error message. use it to create the error info in request context
            set_error(*request_context_, error_code::unsupported_runtime_feature_exception, msg, res);
        }
        scan_info_->status_result(res);
        return;
    }
    scan_info_->blen(blen);
    std::size_t elen{};
    if(auto res = impl::ops::details::encode_key(
        request_context_,
        scan_info_->end_columns(),
        vars,
        *getImplWorkContext().varlen_resource(),
        scan_info_->key_end(),
        elen,
        msg
       );
       res != status::ok) {
        if(res == status::err_type_mismatch) {
            // only on err_type_mismatch, msg is filled with error message. use it to create the error info in request context
            set_error(*request_context_, error_code::unsupported_runtime_feature_exception, msg, res);
        }
        scan_info_->status_result(res);
    }
    scan_info_->elen(elen);
    scan_info_->status_result(status::ok);
    return;
}

void task_context::dump(std::ostream& out, int indent) const noexcept{
    std::string indent_space(indent + 2, ' ');
    abstract::task_context::dump(out,0);
    out << indent_space << "abstract::task_context" << "\n";
    out << indent_space << "request_context_: "
        << (request_context_ ? "non-null" : "null") << '\n';
    out << indent_space << "partition_: " << partition_ << '\n';
    out << indent_space << "io_exchange_map_: "
        << (io_exchange_map_ ? "non-null" : "null") << '\n';
    out << indent_space << "scan_info_:\n";
    if(scan_info_ != nullptr){
       scan_info_->dump(out,4);
    }else{
      out << indent_space << "      null\n";
    }
    out << indent_space << "channel_: "
        << (channel_ ? "non-null" : "null") << '\n';
    out << indent_space << "external_writer_: "
        << (external_writer_ ? "non-null" : "null") << '\n';
    out << indent_space << "sink_index_: " << sink_index_ << std::endl;
}

}
