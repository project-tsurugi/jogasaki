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
    std::unique_ptr<memory::paged_memory_resource> resource_for_records,
    std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables,
    std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data,
    std::shared_ptr<forward_info> info,
    request_context *context
) :
    resource_for_records_(std::move(resource_for_records)),
    resource_for_ptr_tables_(std::move(resource_for_ptr_tables)),
    resource_for_varlen_data_(std::move(resource_for_varlen_data)),
    info_(std::move(info)),
    context_(context)
{}

bool input_partition::write(accessor::record_ref record) {
    initialize_lazy();
    records_->append(record);
    return false;
}

void input_partition::flush() {
    // no-op
}

void input_partition::initialize_lazy() {
    if (! records_) {
        records_ = std::make_unique<data::record_store>(
            resource_for_records_.get(),
            resource_for_varlen_data_.get(),
            info_->record_meta());
    }
}

}  // namespace jogasaki::executor::exchange::forward
