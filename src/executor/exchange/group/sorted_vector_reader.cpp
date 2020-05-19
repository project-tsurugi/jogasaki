
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
#include "sorted_vector_reader.h"

namespace jogasaki::executor::exchange::group {

sorted_vector_reader::sorted_vector_reader(std::shared_ptr<shuffle_info> info, std::vector<std::unique_ptr<input_partition>>& partitions) :
        partitions_(partitions),
        info_(std::move(info)),
        record_size_(info_->record_meta()->record_size()),
        buf_(std::make_unique<char[]>(record_size_)), //NOLINT
        key_comparator_(info_->key_meta().get()) {
    std::size_t count = 0;
    for(auto& p : partitions_) {
        if (!p) continue;
        count += p->tables_count();
    }
    VLOG(1) << "reader initialized to merge " << count << " pointer tables";
}

inline void sorted_vector_reader::read_and_pop() { //NOLINT
    memcpy(buf_.get(), *current_, record_size_);
    ++current_;
}

bool sorted_vector_reader::next_group() {
    init_aggregated_table();
    if (state_ == reader_state::init || state_ == reader_state::after_group) {
        if (current_ == aggregated_pointer_table_.end()) {
            state_ = reader_state::eof;
            return false;
        }
        read_and_pop();
        state_ = reader_state::before_member;
        return true;
    }
    std::abort();
}

[[nodiscard]] accessor::record_ref sorted_vector_reader::get_group() const {
    if (state_ == reader_state::before_member || state_ == reader_state::on_member) {
        return info_->extract_key(accessor::record_ref(buf_.get(), record_size_));
    }
    std::abort();
}

bool sorted_vector_reader::next_member() {
    init_aggregated_table();
    if (state_ == reader_state::before_member) {
        state_ = reader_state::on_member;
        return true;
    }
    if(state_ == reader_state::on_member) {
        if (current_ == aggregated_pointer_table_.end()) {
            state_ = reader_state::after_group;
            return false;
        }
        if (key_comparator_(
                info_->extract_key(accessor::record_ref(buf_.get(), record_size_)),
                info_->extract_key(accessor::record_ref(*current_, record_size_))) == 0) {
            read_and_pop();
            return true;
        }
        state_ = reader_state::after_group;
        return false;
    }
    std::abort();
}

[[nodiscard]] accessor::record_ref sorted_vector_reader::get_member() const {
    if (state_ == reader_state::on_member) {
        return info_->extract_value(accessor::record_ref(buf_.get(), record_size_));
    }
    std::abort();
}

void sorted_vector_reader::release() {
// TODO when multiple readers exist for a source, wait for all readers to complete
    partitions_.clear();
}

void sorted_vector_reader::init_aggregated_table() {
    if(!aggregated_pointer_table_initialized) {
        utils::watch w{};
        w.set_point(0);
        std::size_t count = 0;
        for(auto& p : partitions_) {
            if (!p) continue;
            for(auto& t : *p) {
                count += std::distance(t.begin(), t.end());
            }
        }
        VLOG(1) << "init_aggregated_table: reserving " << count << " pointers";
        aggregated_pointer_table_.reserve(count);
        for(auto& p : partitions_) {
            if (!p) continue;
            for(auto& t : *p) {
                if (t.begin() != t.end()) {
                    for(auto ptr : t) {
                        aggregated_pointer_table_.emplace_back(ptr);
                    }
                }
            }
        }
        w.set_point(1);
        auto sz = info_->record_meta()->record_size();
        std::sort(aggregated_pointer_table_.begin(), aggregated_pointer_table_.end(), [&](auto const&x, auto const& y){
            return key_comparator_(info_->extract_key(accessor::record_ref(x, sz)),
                    info_->extract_key(accessor::record_ref(y, sz))) < 0;
        });
        current_ = aggregated_pointer_table_.begin();
        aggregated_pointer_table_initialized = true;

        w.set_point(2);
        VLOG(1) << "aggregate: total " << w.duration(0, 1) << "ms";
        VLOG(1) << "sort: total " << w.duration(1, 2) << "ms";
    }
}

} // namespace
