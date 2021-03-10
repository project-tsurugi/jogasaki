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
#include "record.h"

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;
using kind = api::field_type_kind;

record::record(
    accessor::record_ref ref,
    maybe_shared_ptr<meta::record_meta> meta
) :
    ref_(ref),
    meta_(std::move(meta))
{}

record::record(maybe_shared_ptr<meta::record_meta> meta) :
    record({}, std::move(meta))
{}

record::runtime_type<kind::int4> impl::record::get_int4(std::size_t index) const {
    return ref_.get_value<runtime_t<k::int4>>(meta_->value_offset(index));
}

record::runtime_type<kind::int8> impl::record::get_int8(std::size_t index) const {
    return ref_.get_value<runtime_t<k::int8>>(meta_->value_offset(index));
}

record::runtime_type<kind::float4> impl::record::get_float4(std::size_t index) const {
    return ref_.get_value<runtime_t<k::float4>>(meta_->value_offset(index));
}

record::runtime_type<kind::float8> impl::record::get_float8(std::size_t index) const {
    return ref_.get_value<runtime_t<k::float8>>(meta_->value_offset(index));
}

record::runtime_type<kind::character> impl::record::get_character(std::size_t index) const {
    return static_cast<field_type_traits<kind::character>::runtime_type>(
        ref_.get_reference<runtime_t<k::character>>(meta_->value_offset(index))
    );
}

bool record::is_null(size_t index) const noexcept {
    return ref_.is_null(meta_->nullity_offset(index));
}

void record::ref(accessor::record_ref r) noexcept {
    ref_ = r;
}

void record::write_to(std::ostream& os) const noexcept {
    os << ref_ << *meta_;
}

accessor::record_ref record::ref() const noexcept {
    return ref_;
}

}

