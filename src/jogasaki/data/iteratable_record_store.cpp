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
#include "iteratable_record_store.h"

namespace jogasaki::data {

iteratable_record_store::iterator &iteratable_record_store::iterator::operator++() {

    pos_ = static_cast<unsigned char*>(pos_) + container_->record_size_; //NOLINT
    if (pos_ >= range_->e_) {
        ++range_;
        if(range_ != container_->ranges_.end()) {
            pos_ = range_->b_;
        } else {
            pos_ = nullptr;
        }
    }
    return *this;
}

const iteratable_record_store::iterator iteratable_record_store::iterator::operator++(int) { //NOLINT
    auto it = *this;
    this->operator++();
    return it;
}

iteratable_record_store::iterator::iterator(const iteratable_record_store &container,
    std::vector<iteratable_record_store::record_range>::iterator range) :
    container_(&container), pos_(range != container_->ranges_.end() ? range->b_ : nullptr), range_(range)
{}

iteratable_record_store::iteratable_record_store(memory::paged_memory_resource *record_resource,
    memory::paged_memory_resource *varlen_resource, std::shared_ptr<meta::record_meta> meta) :
    record_size_(meta->record_size()),
    base_(record_resource, varlen_resource, std::move(meta))
{}

iteratable_record_store::record_pointer iteratable_record_store::append(accessor::record_ref record) {
    auto p = base_.append(record);
    if (prev_ == nullptr || p != static_cast<unsigned char*>(prev_) + record_size_) { //NOLINT
        // starting new range
        ranges_.emplace_back(p, nullptr);
    }
    ranges_.back().e_ = static_cast<unsigned char*>(p) + record_size_; //NOLINT
    prev_ = p;
    return p;
}

std::size_t iteratable_record_store::count() const noexcept {
    return base_.count();
}

bool iteratable_record_store::empty() const noexcept {
    return base_.empty();
}

iteratable_record_store::iterator iteratable_record_store::begin() {
    return iterator{*this, ranges_.begin()};
}

iteratable_record_store::iterator iteratable_record_store::end() {
    return iterator{*this, ranges_.end()};
}

void iteratable_record_store::reset() noexcept {
    base_.reset();
    prev_ = nullptr;
    ranges_.clear();
}
} // namespace
