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
#include "iterable_record_store.h"

#include <memory>
#include <type_traits>
#include <utility>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/record_store.h>

namespace jogasaki::data {

iterable_record_store::iterator &iterable_record_store::iterator::operator++() {
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

const iterable_record_store::iterator iterable_record_store::iterator::operator++(int) { //NOLINT
    auto it = *this;
    this->operator++();
    return it;
}

accessor::record_ref iterable_record_store::iterator::ref() const noexcept {
    return accessor::record_ref{pos_, container_->record_size_};
}

iterable_record_store::iterator::iterator(
    iterable_record_store const& container,
    std::vector<iterable_record_store::record_range>::const_iterator range
) :
    container_(&container),
    pos_(range != container_->ranges_.end() ? range->b_ : nullptr),
    range_(range)
{}

iterable_record_store::value_type iterable_record_store::iterator::operator*() const {
    return ref();
}

iterable_record_store::iterable_record_store(
    memory::paged_memory_resource *record_resource,
    memory::paged_memory_resource *varlen_resource,
    maybe_shared_ptr<meta::record_meta> meta
) :
    record_size_(meta->record_size()),
    base_(record_resource, varlen_resource, std::move(meta))
{}

iterable_record_store::value_type iterable_record_store::append(accessor::record_ref record) {
    auto p = base_.append(record);
    if (prev_ == nullptr || p != static_cast<unsigned char*>(prev_) + record_size_) { //NOLINT
        // starting new range
        ranges_.emplace_back(p, nullptr);
    }
    ranges_.back().e_ = static_cast<unsigned char*>(p) + record_size_; //NOLINT
    prev_ = p;
    return {p, record_size_};
}

std::size_t iterable_record_store::count() const noexcept {
    return base_.count();
}

bool iterable_record_store::empty() const noexcept {
    return base_.empty();
}

iterable_record_store::iterator iterable_record_store::begin() const noexcept {
    return iterator{*this, ranges_.begin()};
}

iterable_record_store::iterator iterable_record_store::end() const noexcept {
    return iterator{*this, ranges_.end()};
}

void iterable_record_store::reset() noexcept {
    base_.reset();
    prev_ = nullptr;
    ranges_.clear();
}

} // namespace
