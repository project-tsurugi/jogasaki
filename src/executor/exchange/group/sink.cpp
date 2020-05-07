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
#include "sink.h"

#include <executor/global.h>
#include <memory/monotonic_paged_memory_resource.h>
#include <executor/record_writer.h>
#include "input_partition.h"
#include "shuffle_info.h"
#include "writer.h"

namespace jogasaki::executor::exchange::group {

sink::sink(std::size_t downstream_partitions, std::shared_ptr<shuffle_info> info) :
        downstream_partitions_(downstream_partitions),
        info_(std::move(info)),
        partitioner_(downstream_partitions_, info_->key_meta())
{}

record_writer& sink::acquire_writer() {
    if (! writer_) {
        writer_ = std::make_unique<group::writer>(downstream_partitions_, info_, partitions_, *this);
    }
    return *writer_;
}

void sink::release_writer(record_writer& writer) {
    if (*writer_ != writer) {
        std::abort();
    }
    writer_.reset();
}

std::vector<std::unique_ptr<input_partition>>& sink::input_partitions() {
    return partitions_;
}

void sink::initialize_lazy(std::size_t partition) {
    if (partitions_.empty()) {
        partitions_.resize(downstream_partitions_);
    }
    if (partitions_[partition]) return;
    partitions_[partition] = std::make_unique<input_partition>(
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::global_page_pool),
            info_);
}

}
