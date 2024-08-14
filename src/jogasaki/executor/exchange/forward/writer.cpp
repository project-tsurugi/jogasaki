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
#include "writer.h"

#include <utility>
#include <glog/logging.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include "forward_info.h"
#include "input_partition.h"
#include "sink.h"

#include <ext/alloc_traits.h>

namespace jogasaki::executor::exchange::forward {

writer::writer(
    std::size_t downstream_partitions,
    std::shared_ptr<forward_info> info,
    sink &owner,
    std::shared_ptr<std::atomic_size_t> write_count
) :
    downstream_partitions_(downstream_partitions),
    info_(std::move(info)),
    owner_(std::addressof(owner)),
    write_count_(std::move(write_count))
{}

bool writer::write(accessor::record_ref rec) {
    if(write_count_ && info_->limit().has_value()) {
        auto cnt = write_count_->load();
        auto limit = info_->limit().value();
        do {
            if(cnt >= limit) {
                return true;
            }
        } while(! write_count_->compare_exchange_weak(cnt, cnt+1));
    }
    initialize_lazy();
    partition_->push(rec);
    return true;
}

void writer::flush() {
    if (partition_) {
        partition_->flush();
    }
}

void writer::release() {
    VLOG_LP(log_trace) << "writer released " << this;
    owner_->release_writer(*this);
}

void writer::initialize_lazy() {
    if (partition_) return;
    partition_ = std::make_shared<input_partition>(
        std::make_unique<memory::fifo_paged_memory_resource>(&global::page_pool()),
        std::make_unique<memory::fifo_paged_memory_resource>(&global::page_pool()),
        std::make_unique<memory::fifo_paged_memory_resource>(&global::page_pool()),
        info_,
        owner_->context()
    );
    owner_->partition(partition_);
}

}  // namespace jogasaki::executor::exchange::forward
