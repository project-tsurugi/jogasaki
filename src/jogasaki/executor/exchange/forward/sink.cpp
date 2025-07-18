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
#include "sink.h"

#include <glog/logging.h>

#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>

#include "forward_info.h"
#include "input_partition.h"
#include "writer.h"

namespace jogasaki::executor::exchange::forward {

sink::sink(
    std::shared_ptr<forward_info> info,
    request_context* context,
    std::shared_ptr<std::atomic_size_t> write_count,
    std::shared_ptr<input_partition> partition
) :
    info_(std::move(info)),
    context_(context),
    write_count_(std::move(write_count)),
    partition_(std::move(partition))
{}

io::record_writer& sink::acquire_writer() {
    if (! writer_) {
        writer_ = std::make_unique<forward::writer>(info_, *this, write_count_, partition_);
        VLOG_LP(log_trace) << "acquire writer from sink:" << this << " writer:" << writer_.get();
    }
    return *writer_;
}

void sink::release_writer(io::record_writer& writer) {
    if (*writer_ != writer) {
        fail_with_exception();
    }
    writer_.reset();

    // after releasing the writer, sink is no longer active
    deactivate();
}

std::shared_ptr<input_partition> const& sink::partition() {
    return partition_;
}

request_context* sink::context() const noexcept {
    return context_;
}

void sink::deactivate() {
    partition_->active().store(false);
}

}  // namespace jogasaki::executor::exchange::forward
