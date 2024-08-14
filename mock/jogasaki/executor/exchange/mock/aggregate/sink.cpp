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

#include <cstdlib>
#include <utility>

#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/request_context.h>

#include "input_partition.h"
#include "shuffle_info.h"
#include "writer.h"

namespace jogasaki::executor::exchange::mock::aggregate {

sink::sink(std::size_t downstream_partitions,
        std::shared_ptr<shuffle_info> info,
        request_context* context
) :
        downstream_partitions_(downstream_partitions),
        info_(std::move(info)),
        context_(context),
        partitioner_(downstream_partitions_, info_->key_meta())
{}

io::record_writer& sink::acquire_writer() {
    if (! writer_) {
        writer_ = std::make_unique<mock::aggregate::writer>(downstream_partitions_, info_, partitions_, *this);
    }
    return *writer_;
}

void sink::release_writer(io::record_writer& writer) {
    if (*writer_ != writer) {
        std::abort();
    }
    writer_.reset();
}

std::vector<std::unique_ptr<input_partition>>& sink::input_partitions() {
    return partitions_;
}

request_context* sink::context() const noexcept {
    return context_;
}

void sink::deactivate() {
    // no-op
}

}  // namespace jogasaki::executor::exchange::mock::aggregate
