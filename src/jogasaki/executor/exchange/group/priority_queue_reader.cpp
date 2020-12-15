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
#include "priority_queue_reader.h"

namespace jogasaki::executor::exchange::group {

using namespace impl;


priority_queue_reader::priority_queue_reader(std::shared_ptr<shuffle_info> info, std::vector<std::unique_ptr<input_partition>>& partitions) :
        partitions_(partitions),
        info_(std::move(info)),
        queue_(iterator_pair_comparator(info_.get())),
        record_size_(info_->record_meta()->record_size()),
        buf_(info_->record_meta()), //NOLINT
        key_comparator_(info_->compare_info()) {
    for(auto& p : partitions_) {
        if (!p) continue;
        for(auto& t : *p) {
            if (t.begin() != t.end()) {
                queue_.emplace(t.begin(), t.end());
            }
        }
    }
    VLOG(1) << "reader initialized to merge " << queue_.size() << " pointer tables";
}

void priority_queue_reader::read_and_pop(iterator it, iterator end) { //NOLINT
    queue_.pop();
    buf_.set(accessor::record_ref(*it, record_size_));
    if (++it != end) {
        queue_.emplace(it, end);
    }
}

bool priority_queue_reader::next_group() {
    if (state_ == reader_state::init || state_ == reader_state::after_group) {
        if (queue_.empty()) {
            state_ = reader_state::eof;
            return false;
        }
        auto it = queue_.top().first;
        auto end = queue_.top().second;
        read_and_pop(it, end);
        state_ = reader_state::before_member;
        return true;
    }
    std::abort();
}

accessor::record_ref priority_queue_reader::get_group() const {
    if (state_ == reader_state::before_member || state_ == reader_state::on_member) {
        if (info_->key_meta()->field_count() == 0) {
            return {};
        }
        return info_->extract_key(buf_.ref());
    }
    std::abort();
}

bool priority_queue_reader::next_member() {
    if (state_ == reader_state::before_member) {
        state_ = reader_state::on_member;
        return true;
    }
    if(state_ == reader_state::on_member) {
        if (queue_.empty()) {
            state_ = reader_state::after_group;
            return false;
        }
        auto it = queue_.top().first;
        auto end = queue_.top().second;
        if (key_comparator_(
                info_->extract_key(buf_.ref()),
                info_->extract_key(accessor::record_ref(*it, record_size_))) == 0) {
            read_and_pop(it, end);
            return true;
        }
        state_ = reader_state::after_group;
        return false;
    }
    std::abort();
}

accessor::record_ref priority_queue_reader::get_member() const {
    if (state_ == reader_state::on_member) {
        return info_->extract_value(accessor::record_ref(buf_.ref()));
    }
    std::abort();
}

void priority_queue_reader::release() {
// TODO when multiple readers exist for a source, wait for all readers to complete
    partitions_.clear();
}

iterator_pair_comparator::iterator_pair_comparator(const shuffle_info *info) :
    info_(info),
    record_size_(info_->record_meta()->record_size()),
    key_comparator_(info_->sort_compare_info()) {}

bool iterator_pair_comparator::operator()(const iterator_pair &x, const iterator_pair &y) {
    auto& it_x = x.first;
    auto& it_y = y.first;
    auto key_x = info_->extract_sort_key(accessor::record_ref(*it_x, record_size_));
    auto key_y = info_->extract_sort_key(accessor::record_ref(*it_y, record_size_));
    return key_comparator_(key_x, key_y) > 0;
}
} // namespace
