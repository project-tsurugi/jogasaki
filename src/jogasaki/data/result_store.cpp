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
#include "result_store.h"

#include <takatori/util/fail.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::data {

using takatori::util::fail;

result_store::store_type & result_store::store(std::size_t index) noexcept {
    return *stores_[index];
}

result_store::store_type const& result_store::store(std::size_t index) const noexcept {
    return *stores_[index];
}

void result_store::initialize(std::size_t partitions, maybe_shared_ptr<meta::record_meta> const& meta) {
    BOOST_ASSERT(stores_.empty());  //NOLINT
    meta_ = meta;
    stores_.reserve(partitions);
    result_record_resources_.reserve(partitions);
    result_varlen_resources_.reserve(partitions);
    for(std::size_t i=0; i < partitions; ++i) {
        auto& res = result_record_resources_.emplace_back(
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool())
        );
        auto& varlen = result_varlen_resources_.emplace_back(
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool())
        );
        stores_.emplace_back(std::make_unique<data::iterable_record_store>( res.get(), varlen.get(), meta));
    }
}

maybe_shared_ptr<meta::record_meta> const& result_store::meta() const noexcept {
    return meta_;
}

std::size_t result_store::partitions() const noexcept {
    return stores_.size();
}

bool result_store::exists(std::size_t index) const noexcept {
    return index < stores_.size() && stores_[index] != nullptr;
}

bool result_store::empty() const noexcept {
    for(auto&& p : stores_) {
        if(p && p->begin() != p->end()) {
            return false;
        }
    }
    return true;
}


result_store::iterator result_store::begin() const noexcept {
    if (empty()) {
        return {};
    }
    std::size_t idx = 0;
    while(idx < stores_.size()) {
        if (exists(idx) && store(idx).begin() != store(idx).end()) {
            return iterator{
                *this,
                idx,
                store(idx).begin()
            };
        }
        ++idx;
    }
    fail();
}

result_store::iterator result_store::end() const noexcept {
    if (empty()) {
        return {};
    }
    std::size_t idx = stores_.size()-1;
    while(idx > 0) {
        if (exists(idx) && store(idx).begin() != store(idx).end()) {
            return {
                *this,
                idx,
                store(idx).end()
            };
        }
        --idx;
    }
    return {
        *this,
        0,
        store(0).end()
    };
}

result_store::iterator::iterator(
    result_store const& container,
    std::size_t store_index,
    iterable_record_store::iterator it
) noexcept:
    container_(std::addressof(container)),
    store_index_(store_index),
    it_(it)
{}

result_store::iterator& result_store::iterator::operator++() {
    BOOST_ASSERT(valid());  //NOLINT
    ++it_;
    if (it_ == container_->store(store_index_).end()) {
        auto idx = store_index_;
        while(++idx < container_->partitions()) {
            if (container_->exists(idx) &&
            container_->store(idx).begin() != container_->store(idx).end()) {
                it_ = container_->store(idx).begin();
                store_index_ = idx;
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

result_store::iterator::value_type result_store::iterator::operator*() {
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

