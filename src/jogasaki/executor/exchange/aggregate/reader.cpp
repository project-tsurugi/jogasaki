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
#include "reader.h"

namespace jogasaki::executor::exchange::aggregate {

static constexpr std::size_t hardware_destructive_interference_size = 64; // replace with std one when C++17 becomes available

reader::reader(std::shared_ptr<shuffle_info> info,
    std::vector<std::unique_ptr<input_partition>>& partitions,
    aggregator_type const& aggregator
) :
    partitions_(partitions),
    info_(std::move(info)),
    key_size_(info_->key_meta()->record_size()),
    value_size_(info_->value_meta()->record_size()),
    key_comparator_(info_->key_meta().get()),
    aggregator_(aggregator)
{
    std::size_t count = 0;
    for(auto& p : partitions_) {
        if (!p) continue;
        for(std::size_t idx = 0, n = p->tables_count(); idx < n; ++idx) {
            if(! p->maps(idx).empty()) {
                ++count;
            }
        }
    }
    maps_.reserve(count);
    for(auto& p : partitions_) {
        if (!p) continue;
        for(std::size_t idx = 0, n = p->tables_count(); idx < n; ++idx) {
            if(! p->maps(idx).empty()) {
                maps_.emplace_back(p->maps(idx));
            }
        }
    }
    iterated_map_ = maps_.begin();
    while(iterated_map_ != maps_.end() && iterated_map_->empty()) {
        ++iterated_map_;
    }
    VLOG(1) << "reader initialized to merge " << count << " hash tables";
}

bool reader::next_group() {
    if (iterated_map_ == maps_.end()) {
        return false;
    }
    if (!iterated_map_->next()) {
        do {
            ++iterated_map_;
        } while(iterated_map_ != maps_.end() && iterated_map_->empty());
        if (iterated_map_ == maps_.end()) {
            return false;
        }
        iterated_map_->next(); // must be successful
    }
    auto key = iterated_map_->key();
    auto value = iterated_map_->value();
    for(auto map = iterated_map_+1; map != maps_.end(); ++map) {
        if(auto it = map->find(key); it != map->end()) {
            aggregator_(info_->value_meta().get(), value, accessor::record_ref(it->second, value_size_));
            map->erase(it);
        }
    }
    on_member_ = false;
    return true;
}

[[nodiscard]] accessor::record_ref reader::get_group() const {
    return iterated_map_->key();
}

bool reader::next_member() {
    if (on_member_) {
        return false;
    }
    on_member_ = true;
    return true;
}

[[nodiscard]] accessor::record_ref reader::get_member() const {
    return iterated_map_->value();
}

void reader::release() {
// TODO when multiple readers exist for a source, wait for all readers to complete
    partitions_.clear();
}

} // namespace
