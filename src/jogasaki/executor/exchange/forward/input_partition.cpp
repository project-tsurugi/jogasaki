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
#include "input_partition.h"

#include <algorithm>
#include <type_traits>
#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/exchange/forward/forward_info.h>
#include <jogasaki/executor/exchange/shuffle/pointer_table.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::exchange::forward {

input_partition::input_partition(
    std::shared_ptr<forward_info> info,
    request_context *context
) :
    info_(std::move(info)),
    context_(context)
{}

input_partition::input_partition(
    std::unique_ptr<memory::fifo_paged_memory_resource> resource,
    std::unique_ptr<memory::fifo_paged_memory_resource> varlen_resource,
    std::shared_ptr<forward_info> info,
    request_context *context
) :
    resource_(std::move(resource)),
    varlen_resource_(std::move(varlen_resource)),
    info_(std::move(info)),
    context_(context)
{}

void input_partition::push(accessor::record_ref record) {
    initialize_lazy();
    records_->push(record);
}

bool input_partition::try_pop(accessor::record_ref& out) {
    initialize_lazy();
    return records_->try_pop(out);
}

void input_partition::flush() {
    // no-op
}

void input_partition::initialize_lazy() {
    if (! resource_) {
        resource_=
            std::make_unique<memory::fifo_paged_memory_resource>(std::addressof(global::page_pool()));
    }
    if (! varlen_resource_) {
        varlen_resource_ =
            std::make_unique<memory::fifo_paged_memory_resource>(std::addressof(global::page_pool()));
    }
    if (! records_) {
        records_ = std::make_unique<data::fifo_record_store>(
            resource_.get(),
            varlen_resource_.get(),
            info_->record_meta());
    }
}

}  // namespace jogasaki::executor::exchange::forward
