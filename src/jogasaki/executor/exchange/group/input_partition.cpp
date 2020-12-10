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
#include "input_partition.h"

#include <jogasaki/request_context.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/executor/exchange/shuffle/pointer_table.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::exchange::group {

input_partition::input_partition(
    std::unique_ptr<memory::paged_memory_resource> resource_for_records,
    std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables,
    std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data, std::shared_ptr<shuffle_info> info,
    request_context *context, std::size_t pointer_table_size
) :
    resource_for_records_(std::move(resource_for_records)),
    resource_for_ptr_tables_(std::move(resource_for_ptr_tables)),
    resource_for_varlen_data_(std::move(resource_for_varlen_data)),
    info_(std::move(info)),
    context_(context),
    comparator_(info_->key_meta().get()),
    max_pointers_(pointer_table_size)
{}

bool input_partition::write(accessor::record_ref record) {
    initialize_lazy();
    auto& table = pointer_tables_.back();
    table.emplace_back(records_->append(record));
    if (table.capacity() == table.size()) {
        flush();
        return true;
    }
    return false;
}

void input_partition::flush() {
    if(!current_pointer_table_active_) return;
    current_pointer_table_active_ = false;
    if(context_->configuration()->noop_pregroup()) return;
    auto sz = info_->record_meta()->record_size();
    auto& table = pointer_tables_.back();
    std::sort(table.begin(), table.end(), [&](auto const&x, auto const& y){
        return comparator_(info_->extract_key(accessor::record_ref(x, sz)),
            info_->extract_key(accessor::record_ref(y, sz))) < 0;
    });
}

input_partition::iterator input_partition::begin() {
    return pointer_tables_.begin();
}

input_partition::iterator input_partition::end() {
    return pointer_tables_.end();
}

std::size_t input_partition::tables_count() const noexcept {
    return pointer_tables_.size();
}

void input_partition::initialize_lazy() {
    if (!records_) {
        records_ = std::make_unique<data::record_store>(
            resource_for_records_.get(),
            resource_for_varlen_data_.get(),
            info_->record_meta());
    }
    if(!current_pointer_table_active_) {
        pointer_tables_.emplace_back(resource_for_ptr_tables_.get(), max_pointers_);
        current_pointer_table_active_ = true;
    }
}
}
