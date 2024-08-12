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
#include "sink.h"

#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/request_context.h>

#include "forward_info.h"
#include "input_partition.h"
#include "writer.h"

namespace jogasaki::executor::exchange::forward {

sink::sink(
    std::size_t downstream_partitions,
    std::shared_ptr<forward_info> info,
    request_context* context
) :
    downstream_partitions_(downstream_partitions),
    info_(std::move(info)),
    context_(context)
{}

io::record_writer& sink::acquire_writer() {
    if (! writer_) {
        writer_ = std::make_unique<forward::writer>();
    }
    return *writer_;
}

void sink::release_writer(io::record_writer& writer) {
    if (*writer_ != writer) {
        fail_with_exception();
    }
    writer_.reset();
}
std::shared_ptr<input_partition> const& sink::partition() {
    return partition_;
}

request_context* sink::context() const noexcept {
    return context_;
}

}  // namespace jogasaki::executor::exchange::forward
