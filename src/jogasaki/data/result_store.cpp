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
#include "result_store.h"

#include <utility>
#include <boost/assert.hpp>

#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::data {

result_store::partition_type & result_store::partition(std::size_t index) noexcept {
    return *partitions_[index];
}

result_store::partition_type const& result_store::partition(std::size_t index) const noexcept {
    return *partitions_[index];
}

void result_store::add_partition_internal(maybe_shared_ptr<meta::record_meta> const& meta) {
    auto& res = result_record_resources_.emplace_back(
        std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool())
    );
    auto& varlen = result_varlen_resources_.emplace_back(
        std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool())
    );
    partitions_.emplace_back(std::make_unique<data::iterable_record_store>( res.get(), varlen.get(), meta));
}

void result_store::initialize(std::size_t partitions, maybe_shared_ptr<meta::record_meta> const& meta) {
    BOOST_ASSERT(partitions_.empty());  //NOLINT
    meta_ = meta;
    partitions_.reserve(partitions);
    result_record_resources_.reserve(partitions);
    result_varlen_resources_.reserve(partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        add_partition_internal(meta);
    }
}

std::size_t result_store::add_partition() {
    add_partition_internal(meta_);
    return partitions_.size()-1;
}

maybe_shared_ptr<meta::record_meta> const& result_store::meta() const noexcept {
    return meta_;
}

std::size_t result_store::partitions() const noexcept {
    return partitions_.size();
}

bool result_store::exists(std::size_t index) const noexcept {
    return index < partitions_.size() && partitions_[index] != nullptr;
}

bool result_store::empty() const noexcept {
    for(auto&& p : partitions_) {
        if(p && p->begin() != p->end()) {
            return false;
        }
    }
    return true;
}

result_store::iterator result_store::begin() const {
    if (empty()) {
        return {};
    }
    std::size_t idx = 0;
    while(idx < partitions_.size()) {
        if (exists(idx) && partition(idx).begin() != partition(idx).end()) {
            return iterator{
                *this,
                idx,
                partition(idx).begin()
            };
        }
        ++idx;
    }
    fail_with_exception();
}

result_store::iterator result_store::end() const noexcept {
    if (empty()) {
        return {};
    }
    std::size_t idx = partitions_.size()-1;
    while(idx > 0) {
        if (exists(idx) && partition(idx).begin() != partition(idx).end()) {
            return {
                *this,
                idx,
                partition(idx).end()
            };
        }
        --idx;
    }
    return {
        *this,
        0,
        partition(0).end()
    };
}

void result_store::clear_partition(std::size_t index) {
    if(index >= partitions_.size()) {
        return;
    }
    partitions_[index].reset();
    result_record_resources_[index].reset();
    result_varlen_resources_[index].reset();
}

void result_store::initialize(maybe_shared_ptr<meta::record_meta> meta) {
    meta_ = std::move(meta);
}

result_store::iterator::iterator(
    result_store const& container,
    std::size_t partition_index,
    iterable_record_store::iterator it
) noexcept:
    container_(std::addressof(container)),
    partition_index_(partition_index),
    it_(it)
{}

result_store::iterator& result_store::iterator::operator++() {
    BOOST_ASSERT(valid());  //NOLINT
    ++it_;
    if (it_ == container_->partition(partition_index_).end()) {
        auto idx = partition_index_;
        while(++idx < container_->partitions()) {
            if (container_->exists(idx) &&
            container_->partition(idx).begin() != container_->partition(idx).end()) {
                it_ = container_->partition(idx).begin();
                partition_index_ = idx;
                return *this;
            }
        }
        // if not found, *this becomes end iterator
    }
    return *this;
}

result_store::iterator const result_store::iterator::operator++(int) {  //NOLINT
    auto ret = *this;
    ++*this;
    return ret;
}

result_store::iterator::value_type result_store::iterator::operator*() const {
    return ref();
}

accessor::record_ref result_store::iterator::ref() const noexcept {
    BOOST_ASSERT(valid());  //NOLINT
    return it_.ref();
}


data::iterable_record_store::iterator empty_iterator() {
    static data::iterable_record_store empty_store{}; // dummy store to provide empty iterator
    return empty_store.begin();
}


result_store::iterator::iterator() noexcept:
    it_(empty_iterator())
{}

bool result_store::iterator::valid() const noexcept {
    return container_ != nullptr;
}

}

