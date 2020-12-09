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

namespace jogasaki::executor::exchange::aggregate {

input_partition::input_partition(
    std::unique_ptr<memory::paged_memory_resource> resource_for_keys,
    std::unique_ptr<memory::paged_memory_resource> resource_for_values,
    std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data,
    std::unique_ptr<memory::paged_memory_resource> resource_for_hash_tables,
    std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables,
    std::shared_ptr<aggregate_info> info,
    std::size_t initial_hash_table_size, std::size_t pointer_table_size
) noexcept :
    resource_for_keys_(std::move(resource_for_keys)),
    resource_for_values_(std::move(resource_for_values)),
    resource_for_varlen_data_(std::move(resource_for_varlen_data)),
    resource_for_hash_tables_(std::move(resource_for_hash_tables)),
    resource_for_ptr_tables_(std::move(resource_for_ptr_tables)),
    info_(std::move(info)),
    comparator_(info_->key_meta().get()),
    initial_hash_table_size_(initial_hash_table_size),
    max_pointers_(pointer_table_size),
    key_buf_(info_->key_meta())
{}

bool input_partition::write(accessor::record_ref record) {
    initialize_lazy();
    auto& key_meta = info_->key_meta();
    auto key_indices = info_->key_indices();
    auto key_buf = key_buf_.ref();
    auto& record_meta = info_->record_meta();
    for(std::size_t i=0, n = key_indices.size(); i < n; ++i) {
        auto input_record_field = key_indices[i];
        utils::copy_nullable_field(
            record_meta->at(input_record_field),
            key_buf,
            key_meta->value_offset(i),
            key_meta->nullity_offset(i),
            record,
            record_meta->value_offset(input_record_field),
            record_meta->nullity_offset(input_record_field),
            keys_->varlen_resource()
        );
    }
    auto& value_meta = info_->value_meta();
    accessor::record_ref value{};
    bool initial = false;
    if (auto it = hash_table_->find(key_buf.data()); it != hash_table_->end()) {
        value = accessor::record_ref(it->second, info_->value_meta()->record_size());
    } else {
        initial = true;
        value = accessor::record_ref{values_->allocate_record(), value_meta->record_size()};
        accessor::record_ref key{keys_->allocate_record(), key_meta->record_size()};
        keys_->copier()(key, key_buf);
        key.set_value<void*>(info_->key_meta()->value_offset(info_->key_meta()->field_count()-1), value.data());
        hash_table_->emplace(keys_->append(key), values_->append(value));
        if(! current_table_active_) {
            pointer_tables_.emplace_back(resource_for_ptr_tables_.get(), max_pointers_);
            current_table_active_ = true;
        }
        auto& table = pointer_tables_.back();
        table.emplace_back(key.data());
    }
    for(std::size_t i=0, n = info_->value_specs().size(); i < n; ++i) {
        auto& vspec = info_->value_specs()[i];
        auto& aggregator = vspec.aggregator();
        aggregator(value, value_meta->value_offset(i), value_meta->nullity_offset(i), initial, record, info_->aggregators_args(i));
    }
    if (hash_table_->load_factor() > load_factor_bound) {
        flush();
        return true;
    }
    // TODO predict and avoid unexpected reallocation (e.g. all neighbors occupied) where memory allocator raises bad_alloc
    return false;
}

void input_partition::flush() {
    if(! current_table_active_) return;
    current_table_active_ = false;
    auto sz = info_->record_meta()->record_size();
    auto& table = pointer_tables_.back();
    std::sort(table.begin(), table.end(), [&](auto const&x, auto const& y){
        return comparator_(
            accessor::record_ref(x, sz),
            accessor::record_ref(y, sz)) < 0;
    });
}

void input_partition::initialize_lazy() {
    if (! keys_) {
        keys_ = std::make_unique<data::record_store>(
            resource_for_keys_.get(),
            resource_for_varlen_data_.get(),
            info_->key_meta());
    }
    if (! values_) {
        values_ = std::make_unique<data::record_store>(
            resource_for_values_.get(),
            resource_for_varlen_data_.get(),
            info_->value_meta());
    }
    if(! hash_table_) {
        hash_table_ = std::make_unique<hash_table>(initial_hash_table_size_,
            hash{info_->key_meta().get()},
            impl::key_eq{comparator_, info_->key_meta()->record_size()},
            hash_table_allocator{resource_for_hash_tables_.get()}
        );
    }
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

bool input_partition::empty(std::size_t index) const noexcept {
    return pointer_tables_[index].empty();
}

void input_partition::release_hashtable() noexcept {
    hash_table_.reset();
    resource_for_hash_tables_.reset();
}
}
