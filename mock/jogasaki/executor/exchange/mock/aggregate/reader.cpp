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
#include "reader.h"

#include <glog/logging.h>

#include <jogasaki/logging.h>

namespace jogasaki::executor::exchange::mock::aggregate {

reader::reader(
    std::shared_ptr<shuffle_info> info,
    std::vector<std::unique_ptr<input_partition>>& partitions,
    aggregator_type const& aggregator
) :
    partitions_(partitions),
    info_(std::move(info)),
    aggregator_(aggregator),
    key_size_(info_->key_meta()->record_size()),
    value_size_(info_->value_meta()->record_size())
{
    std::size_t count = 0;
    for(auto& p : partitions_) {
        if (!p) continue;
        for(std::size_t idx = 0, n = p->tables_count(); idx < n; ++idx) {
            if(!p->empty(idx)) {
                ++count;
            }
        }
    }
    tables_.reserve(count);
    for(auto& p : partitions_) {
        if (!p) continue;
        for(std::size_t idx = 0, n = p->tables_count(); idx < n; ++idx) {
            if(!p->empty(idx)) {
                tables_.emplace_back(p->table_at(idx));
            }
        }
    }
    iterated_table_ = tables_.begin();
    while(iterated_table_ != tables_.end() && iterated_table_->empty()) {
        ++iterated_table_;
    }
    VLOG(log_debug) << "reader initialized to merge " << count << " hash tables";
}

bool reader::next_group() {
    if (iterated_table_ == tables_.end()) {
        return false;
    }
    if (!iterated_table_->next()) {
        do {
            ++iterated_table_;
        } while(iterated_table_ != tables_.end() && iterated_table_->empty());
        if (iterated_table_ == tables_.end()) {
            return false;
        }
        (void)iterated_table_->next(); // must be successful
    }
    auto key = iterated_table_->key();
    auto value = iterated_table_->value();
    std::size_t precalculated_hash = iterated_table_->calculate_hash(key);
    for(auto table = iterated_table_+1; table != tables_.end(); ++table) {
        if(auto it = table->find(key, precalculated_hash); it != table->end()) {
            aggregator_(info_->value_meta().get(), value, accessor::record_ref(it->second, value_size_));
            table->erase(it);
        }
    }
    on_member_ = false;
    return true;
}

accessor::record_ref reader::get_group() const {
    return iterated_table_->key();
}

bool reader::next_member() {
    if (on_member_) {
        return false;
    }
    on_member_ = true;
    return true;
}

accessor::record_ref reader::get_member() const {
    return iterated_table_->value();
}

void reader::release() {
// TODO when multiple readers exist for a source, wait for all readers to complete
    partitions_.clear();
}

} // namespace
