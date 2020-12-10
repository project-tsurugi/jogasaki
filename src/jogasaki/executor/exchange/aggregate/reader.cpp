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

#include <glog/logging.h>

namespace jogasaki::executor::exchange::aggregate {

reader::reader(
    std::shared_ptr<aggregate_info> info,
    std::vector<std::unique_ptr<input_partition>>& partitions
) :
    partitions_(partitions),
    info_(std::move(info)),
    queue_(impl::iterator_pair_comparator(info_.get())),
    key_size_(info_->key_meta()->record_size()),
    value_size_(info_->value_meta()->record_size()),
    key_buf_(info_->key_meta()), //NOLINT
    value_buf_(info_->value_meta()), //NOLINT
    key_comparator_(info_->key_meta().get()),
    pointer_field_offset_(info_->key_meta()->value_offset(info_->key_meta()->field_count()-1)) {
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

void reader::read_and_pop(impl::iterator it, impl::iterator end) { //NOLINT
    queue_.pop();
    key_buf_.set(accessor::record_ref(*it, key_size_));
    if (++it != end) {
        queue_.emplace(it, end);
    }
}

bool reader::next_group() {
    if (state_ == reader_state::init || state_ == reader_state::after_group) {
        if (queue_.empty()) {
            state_ = reader_state::eof;
            return false;
        }
        auto it = queue_.top().first;
        auto end = queue_.top().second;
        read_and_pop(it, end);
        bool initial = true;
        internal_on_member_ = false;
        while(internal_next_member()) {
            auto src = internal_get_member();
            auto tgt = value_buf_.ref();
            for(std::size_t i=0, n = info_->value_specs().size(); i < n; ++i) {
                auto& vspec = info_->value_specs()[i];
                // FIXME use intermediate aggregator
                auto& aggregator = vspec.aggregator();
                aggregator(
                    tgt,
                    info_->target_field_locator(i),
                    initial,
                    src,
                    sequence_view{&info_->target_field_locator(i)}
                );
            }
            initial = false;
        }
        state_ = reader_state::before_member;
        return true;
    }
    std::abort();
}

accessor::record_ref reader::get_group() const {
    if (state_ == reader_state::before_member || state_ == reader_state::on_member) {
        return key_buf_.ref();
    }
    std::abort();
}

bool reader::internal_next_member() {
    if (! internal_on_member_) {
        internal_on_member_ = true;
        return true;
    }
    if (queue_.empty()) {
        return false;
    }
    auto it = queue_.top().first;
    auto end = queue_.top().second;
    if (key_comparator_(
        key_buf_.ref(),
        accessor::record_ref(*it, key_size_)) == 0) {
        read_and_pop(it, end);
        return true;
    }
    return false;
}

void* reader::value_pointer(accessor::record_ref ref) const {
    return ref.get_value<void*>(pointer_field_offset_);
}

accessor::record_ref reader::get_member() const {
    if (state_ == reader_state::on_member) {
        return value_buf_.ref();
    }
    fail();
}

bool reader::next_member() {
    if (state_ == reader_state::before_member) {
        state_ = reader_state::on_member;
        return true;
    }
    if(state_ == reader_state::on_member) {
        state_ = reader_state::after_group;
        return false;
    }
    std::abort();
}

accessor::record_ref reader::internal_get_member() const {
    auto p = value_pointer(key_buf_.ref());
    if(! p) fail();
    return accessor::record_ref{p, value_size_};
}

void reader::release() {
// TODO when multiple readers exist for a source, wait for all readers to complete
    partitions_.clear();
}

} // namespace
