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
#include "partitioner.h"

#include <cstddef>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

partitioner::partitioner(
    std::size_t partitions,
    maybe_shared_ptr<meta::record_meta> meta
) noexcept:
    partitions_(partitions),
    meta_(std::move(meta))
{}

std::size_t partitioner::operator()(accessor::record_ref key) const noexcept {
    static const std::size_t p = 18446744073709551557ULL; // arbitrary prime in int64_t
    std::size_t h = 0;
    for(std::size_t i = 0, n = meta_->field_count(); i < n; ++i) {
        h += field_hash(key, i);
        h *= i == 0 ? 1 : p;
    }
    return h % partitions_;
}

std::size_t partitioner::field_hash(accessor::record_ref key, std::size_t field_index) const {
    if(meta_->nullable(field_index) && key.is_null(meta_->nullity_offset(field_index))) {
        return static_cast<std::size_t>(-1);
    }
    auto type = meta_->at(field_index);
    auto offset = meta_->value_offset(field_index);
    switch(type.kind()) {
        case meta::field_type_kind::boolean: return std::hash<runtime_t<kind::boolean>>()(key.get_value<runtime_t<kind::boolean>>(offset));
        case meta::field_type_kind::int1: return std::hash<runtime_t<kind::int1>>()(key.get_value<runtime_t<kind::int1>>(offset));
        case meta::field_type_kind::int2: return std::hash<runtime_t<kind::int2>>()(key.get_value<runtime_t<kind::int2>>(offset));
        case meta::field_type_kind::int4: return std::hash<runtime_t<kind::int4>>()(key.get_value<runtime_t<kind::int4>>(offset));
        case meta::field_type_kind::int8: return std::hash<runtime_t<kind::int8>>()(key.get_value<runtime_t<kind::int8>>(offset));
        case meta::field_type_kind::float4: return std::hash<runtime_t<kind::float4>>()(key.get_value<runtime_t<kind::float4>>(offset));
        case meta::field_type_kind::float8: return std::hash<runtime_t<kind::float8>>()(key.get_value<runtime_t<kind::float8>>(offset));
        case meta::field_type_kind::character: return std::hash<runtime_t<kind::character>>()(key.get_value<runtime_t<kind::character>>(offset));
        case meta::field_type_kind::octet: return std::hash<runtime_t<kind::octet>>()(key.get_value<runtime_t<kind::octet>>(offset));
        case meta::field_type_kind::decimal: return std::hash<runtime_t<kind::decimal>>()(key.get_value<runtime_t<kind::decimal>>(offset));
        case meta::field_type_kind::date: return std::hash<runtime_t<kind::date>>()(key.get_value<runtime_t<kind::date>>(offset));
        case meta::field_type_kind::time_of_day: return std::hash<runtime_t<kind::time_of_day>>()(key.get_value<runtime_t<kind::time_of_day>>(offset));
        case meta::field_type_kind::time_point: return std::hash<runtime_t<kind::time_point>>()(key.get_value<runtime_t<kind::time_point>>(offset));
        case meta::field_type_kind::pointer: return static_cast<std::size_t>(-1); // ignore internal field
        default:
            // TODO implement other types
            std::abort();
    }
}

}
