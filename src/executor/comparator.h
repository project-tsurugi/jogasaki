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
#pragma once

#include <cstddef>
#include <functional>
#include <meta/record_meta.h>

namespace jogasaki::executor {

/**
 * @brief record comparator
 */
class comparator {
public:
    /**
     * @brief construct empty object
     */
    comparator() = default;

    /**
     * @brief construct new object
     * @param meta schema information for the records to be compared
     * @attention record_meta is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    explicit comparator(meta::record_meta const* meta) noexcept : meta_(meta) {}

    /**
     * @brief compare function
     * @param a 1st argument for comparison
     * @param b 2nd argument for comparison
     * @return negative if a < b
     * @return positive if a > b
     * @return zero if a is equivalent to b
     */
    int operator()(accessor::record_ref const& a, accessor::record_ref const& b) const noexcept {
        for(std::size_t i = 0, n = meta_->field_count(); i < n; ++i) {
            auto res = compare_field(a, b, i);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }

private:
    meta::record_meta const* meta_{};

    template <meta::field_type_kind Kind>
    using runtime_type = typename meta::field_type_traits<Kind>::runtime_type;
    using kind = meta::field_type_kind;

    template <kind K>
    struct field_comparator {
        int operator()(accessor::record_ref const& a, accessor::record_ref const& b, std::size_t offset) {
            using rtype = runtime_type<K>;
            auto l = a.get_value<rtype>(offset);
            auto r = b.get_value<rtype>(offset);
            if (std::less<rtype>{}(l, r)) return -1;
            if (std::less<rtype>{}(r, l)) return 1;
            return 0;
        }
    };

    [[nodiscard]] int compare_field(accessor::record_ref const& a, accessor::record_ref const& b, std::size_t field_index) const {
        auto& type = meta_->at(field_index);
        auto offset = meta_->value_offset(field_index);
        switch(type.kind()) {
            case meta::field_type_kind::boolean: return field_comparator<kind::boolean>{}(a, b, offset);
            case meta::field_type_kind::int1: return field_comparator<kind::int1>{}(a, b, offset);
            case meta::field_type_kind::int2: return field_comparator<kind::int2>{}(a, b, offset);
            case meta::field_type_kind::int4: return field_comparator<kind::int4>{}(a, b, offset);
            case meta::field_type_kind::int8: return field_comparator<kind::int8>{}(a, b, offset);
            case meta::field_type_kind::float4: return field_comparator<kind::float4>{}(a, b, offset);
            case meta::field_type_kind::float8: return field_comparator<kind::float8>{}(a, b, offset);
            case meta::field_type_kind::character: return field_comparator<kind::character>{}(a, b, offset);
            default:
                // TODO implement other types
                std::abort();
        }
        std::abort();
    }
};

}
