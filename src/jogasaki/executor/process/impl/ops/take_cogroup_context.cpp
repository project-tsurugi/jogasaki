/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "take_cogroup_context.h"

#include <type_traits>
#include <utility>
#include <boost/assert.hpp>

#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

using checkpoint = memory::lifo_paged_memory_resource::checkpoint;

group_input::group_input(
    io::group_reader& reader,
    std::unique_ptr<data::iterable_record_store> store,
    memory::lifo_paged_memory_resource* resource,
    memory::lifo_paged_memory_resource* varlen_resource,
    maybe_shared_ptr <meta::group_meta> meta
) :
    reader_(std::addressof(reader)),
    store_(std::move(store)),

    resource_(resource),
    varlen_resource_(varlen_resource),
    resource_last_checkpoint_(resource_ ? resource_->get_checkpoint() : checkpoint{}),
    varlen_resource_last_checkpoint_(varlen_resource_ ? varlen_resource_->get_checkpoint() : checkpoint{}),

    meta_(std::move(meta)),
    key_size_(meta_->key().record_size()),
    current_key_(meta_->key_shared()),
    next_key_(meta_->key_shared())
{}

accessor::record_ref group_input::current_key() const noexcept {
    BOOST_ASSERT(values_filled_);  //NOLINT
    return current_key_.ref();
}

accessor::record_ref group_input::next_key() const noexcept {
    BOOST_ASSERT(next_key_read_);  //NOLINT
    BOOST_ASSERT(! reader_eof_);  //NOLINT
    return next_key_.ref();
}

const maybe_shared_ptr <meta::group_meta>& group_input::meta() {
    return meta_;
}

bool group_input::eof() const noexcept {
    return reader_eof_;
}

bool group_input::filled() const noexcept {
    return values_filled_;
}

bool group_input::next_key_read() const noexcept {
    return next_key_read_;
}

data::iterable_record_store::iterator group_input::begin() {
    return store_->begin();
}

data::iterable_record_store::iterator group_input::end() {
    return store_->end();
}

bool group_input::read_next_key() {
    if(! reader_->next_group()) {
        next_key_read_ = false;
        reader_eof_ = true;
        return false;
    }
    next_key_.set(reader_->get_group());
    next_key_read_ = true;
    reader_eof_ = false;
    return true;
}

void group_input::fill() noexcept {
    BOOST_ASSERT(next_key_read_);  //NOLINT
    BOOST_ASSERT(! reader_eof_);  //NOLINT
    while(reader_->next_member()) {
        auto rec = reader_->get_member();
        store_->append(rec);
    }
    current_key_.set(next_key_.ref());
    next_key_read_ = false;
    values_filled_ = true;
}

void group_input::reset_values() {
    if (values_filled_) {
        store_->reset();
        if (resource_) {
            resource_->deallocate_after(resource_last_checkpoint_);
            resource_last_checkpoint_ = resource_->get_checkpoint();
        }
        if (varlen_resource_) {
            varlen_resource_->deallocate_after(varlen_resource_last_checkpoint_);
            varlen_resource_last_checkpoint_ = varlen_resource_->get_checkpoint();
        }
        values_filled_ = false;
    }
}

group_input_comparator::group_input_comparator(
    std::vector<group_input>* inputs
) :
    inputs_(inputs)
{}

bool group_input_comparator::operator()(
    group_input_comparator::input_index const& x,
    group_input_comparator::input_index const& y
) {
    auto& l = inputs_->operator[](x);
    auto& r = inputs_->operator[](y);
    compare_info cinfo{
        l.meta()->key(),
        r.meta()->key(),
    };
    comparator key_comparator{cinfo};
    return key_comparator(l.next_key(), r.next_key()) > 0;
}
} // namespace details

take_cogroup_context::take_cogroup_context(
    abstract::task_context* ctx,
    variable_table& variables,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource)
{}

operator_kind take_cogroup_context::kind() const noexcept {
    return operator_kind::take_cogroup;
}

void take_cogroup_context::release() {
    for(auto* r : readers_) {
        if(r) {
            r->release();
        }
    }
}

}


