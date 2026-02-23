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
#include "record.h"

#include <type_traits>
#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

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

record::runtime_type<kind::boolean> impl::record::get_boolean(std::size_t index) const {
    return ref_.get_value<runtime_t<k::boolean>>(meta_->value_offset(index));
}

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

record::runtime_type<kind::octet> impl::record::get_octet(std::size_t index) const {
    return static_cast<field_type_traits<kind::octet>::runtime_type>(
        ref_.get_reference<runtime_t<k::octet>>(meta_->value_offset(index))
    );
}

record::runtime_type<kind::decimal> impl::record::get_decimal(std::size_t index) const {
    return ref_.get_value<runtime_t<k::decimal>>(meta_->value_offset(index));
}

record::runtime_type<kind::date> impl::record::get_date(std::size_t index) const {
    return ref_.get_value<runtime_t<k::date>>(meta_->value_offset(index));
}

record::runtime_type<kind::time_of_day> impl::record::get_time_of_day(std::size_t index) const {
    return ref_.get_value<runtime_t<k::time_of_day>>(meta_->value_offset(index));
}

record::runtime_type<kind::time_point> impl::record::get_time_point(std::size_t index) const {
    return ref_.get_value<runtime_t<k::time_point>>(meta_->value_offset(index));
}

bool record::is_null(size_t index) const noexcept {
    return ref_.is_null(meta_->nullity_offset(index));
}

void record::ref(accessor::record_ref r) noexcept {
    ref_ = r;
}

void record::write_to(std::ostream& os) const {
    os << ref_ << *meta_;
}

accessor::record_ref record::ref() const noexcept {
    return ref_;
}

}

