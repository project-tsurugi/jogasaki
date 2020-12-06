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
#include "comparator.h"

namespace jogasaki::executor {

comparator::comparator(const meta::record_meta *l_meta, const meta::record_meta *r_meta) noexcept:
    l_meta_(l_meta),
    r_meta_(r_meta)
{
    BOOST_ASSERT(l_meta_->field_count() == r_meta_->field_count());  //NOLINT
    for(std::size_t i=0, n = l_meta_->field_count(); i < n; ++i) {
        (void)i;
        BOOST_ASSERT(l_meta_->at(i) == r_meta_->at(i));  //NOLINT
    }
}

int comparator::operator()(const accessor::record_ref &a, const accessor::record_ref &b) const noexcept {
    for(std::size_t i = 0, n = l_meta_->field_count(); i < n; ++i) {
        auto res = compare_field(a, b, i);
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

template <meta::field_type_kind Kind>
using runtime_type = typename meta::field_type_traits<Kind>::runtime_type;
using kind = meta::field_type_kind;

template <kind K>
struct field_comparator {
    int operator()(accessor::record_ref const& a, accessor::record_ref const& b, std::size_t l_offset, std::size_t r_offset) {
        using rtype = runtime_type<K>;
        auto l = a.get_value<rtype>(l_offset);
        auto r = b.get_value<rtype>(r_offset);
        if (std::less<rtype>{}(l, r)) return -1;
        if (std::less<rtype>{}(r, l)) return 1;
        return 0;
    }
};

int
comparator::compare_field(const accessor::record_ref &a, const accessor::record_ref &b, std::size_t field_index) const {
    auto& type = l_meta_->at(field_index);
    if(type.kind() == kind::pointer) return 0; // ignore internal fields
    auto l_nullable = l_meta_->nullable(field_index);
    auto r_nullable = r_meta_->nullable(field_index);
    if(l_nullable || r_nullable) {
        bool a_null = l_nullable && a.is_null(l_meta_->nullity_offset(field_index));
        bool b_null = r_nullable && b.is_null(r_meta_->nullity_offset(field_index));
        if (a_null != b_null) {
            return a_null ? -1 : 1;
        }
        if (a_null) {
            return 0;
        }
    }
    auto l_offset = l_meta_->value_offset(field_index);
    auto r_offset = r_meta_->value_offset(field_index);
    switch(type.kind()) {
        case meta::field_type_kind::boolean: return field_comparator<kind::boolean>{}(a, b, l_offset, r_offset);
        case meta::field_type_kind::int1: return field_comparator<kind::int1>{}(a, b, l_offset, r_offset);
        case meta::field_type_kind::int2: return field_comparator<kind::int2>{}(a, b, l_offset, r_offset);
        case meta::field_type_kind::int4: return field_comparator<kind::int4>{}(a, b, l_offset, r_offset);
        case meta::field_type_kind::int8: return field_comparator<kind::int8>{}(a, b, l_offset, r_offset);
        case meta::field_type_kind::float4: return field_comparator<kind::float4>{}(a, b, l_offset, r_offset);
        case meta::field_type_kind::float8: return field_comparator<kind::float8>{}(a, b, l_offset, r_offset);
        case meta::field_type_kind::character: return field_comparator<kind::character>{}(a, b, l_offset, r_offset);
        default:
            // TODO implement other types
            std::abort();
    }
    std::abort();
}
}
