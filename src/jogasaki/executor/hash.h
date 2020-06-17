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

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::executor {

/**
 * @brief record hash
 */
class hash {
public:
    using hash_value = std::size_t;
    /**
     * @brief construct empty object
     */
    hash() = default;

    /**
     * @brief construct new object
     * @param meta schema information for the records to be compared
     * @attention record_meta is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    explicit hash(meta::record_meta const* meta) noexcept : meta_(meta) {}

    /**
     * @brief hash function
     * @param record the record to calculate the hash
     * @return hash of the record
     */
    hash_value operator()(accessor::record_ref const& record) const noexcept {
        hash_value h{};
        for(std::size_t i = 0, n = meta_->field_count(); i < n; ++i) {
            h *= 31;
            h += hash_field(record, i);
        }
        return h;
    }

    /**
     * @brief hash function
     * @param ptr the pointer to record data to calculate the hash
     * @return hash of the record
     */
    hash_value operator()(void* ptr) const noexcept {
        return operator()(accessor::record_ref(ptr, meta_->record_size()));
    }
private:
    meta::record_meta const* meta_{};

    template <meta::field_type_kind Kind>
    using runtime_type = typename meta::field_type_traits<Kind>::runtime_type;
    using kind = meta::field_type_kind;

    template <kind K>
    struct hash_calculator {
        hash_value operator()(accessor::record_ref const& a, std::size_t offset) {
            using rtype = runtime_type<K>;
            auto l = a.get_value<rtype>(offset);
            return std::hash<rtype>{}(l);
        }
    };

    [[nodiscard]] hash_value hash_field(accessor::record_ref const& record, std::size_t field_index) const {
        auto& type = meta_->at(field_index);
        auto offset = meta_->value_offset(field_index);
        switch(type.kind()) {
            case meta::field_type_kind::boolean: return hash_calculator<kind::boolean>{}(record, offset);
            case meta::field_type_kind::int1: return hash_calculator<kind::int1>{}(record, offset);
            case meta::field_type_kind::int2: return hash_calculator<kind::int2>{}(record, offset);
            case meta::field_type_kind::int4: return hash_calculator<kind::int4>{}(record, offset);
            case meta::field_type_kind::int8: return hash_calculator<kind::int8>{}(record, offset);
            case meta::field_type_kind::float4: return hash_calculator<kind::float4>{}(record, offset);
            case meta::field_type_kind::float8: return hash_calculator<kind::float8>{}(record, offset);
            case meta::field_type_kind::character: return hash_calculator<kind::character>{}(record, offset);
            default:
                // TODO implement other types
                std::abort();
        }
        std::abort();
    }
};

}
