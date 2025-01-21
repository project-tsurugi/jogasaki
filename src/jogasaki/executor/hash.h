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
    [[nodiscard]] hash_value operator()(accessor::record_ref const& record) const noexcept {
        static const std::size_t p = 18446744073709551557ULL; // arbitrary prime in uint64_t
        hash_value h{};
        for(std::size_t i = 0, n = meta_->field_count(); i < n; ++i) {
            h *= p;
            h += hash_field(record, i);
        }
        return h;
    }

    /**
     * @brief hash function
     * @param ptr the pointer to record data to calculate the hash
     * @return hash of the record
     */
    [[nodiscard]] hash_value operator()(void* ptr) const noexcept {
        return operator()(accessor::record_ref(ptr, meta_->record_size()));
    }

private:
    meta::record_meta const* meta_{};

    using kind = meta::field_type_kind;

    template <kind K>
    struct hash_calculator {
        hash_value operator()(accessor::record_ref const& a, std::size_t offset) {
            using rtype = runtime_t<K>;
            auto l = a.get_value<rtype>(offset);
            return std::hash<rtype>{}(l);
        }
    };

    [[nodiscard]] hash_value hash_field(accessor::record_ref const& record, std::size_t field_index) const {
        if(meta_->nullable(field_index) && record.is_null(meta_->nullity_offset(field_index))) {
            return static_cast<hash_value>(-1);
        }
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
            case meta::field_type_kind::octet: return hash_calculator<kind::octet>{}(record, offset);
            case meta::field_type_kind::decimal: return hash_calculator<kind::decimal>{}(record, offset);
            case meta::field_type_kind::date: return hash_calculator<kind::date>{}(record, offset);
            case meta::field_type_kind::time_of_day: return hash_calculator<kind::time_of_day>{}(record, offset);
            case meta::field_type_kind::time_point: return hash_calculator<kind::time_point>{}(record, offset);
            case meta::field_type_kind::blob: return static_cast<hash_value>(-1);
            case meta::field_type_kind::clob: return static_cast<hash_value>(-1);
            case meta::field_type_kind::unknown: return static_cast<hash_value>(-1);
            case meta::field_type_kind::undefined: return static_cast<hash_value>(-1);

            case meta::field_type_kind::bit: return static_cast<hash_value>(-1); // not supported yet // TODO
            case meta::field_type_kind::time_interval: return static_cast<hash_value>(-1); // not supported yet // TODO
            case meta::field_type_kind::array: return static_cast<hash_value>(-1); // not supported yet // TODO
            case meta::field_type_kind::record: return static_cast<hash_value>(-1); // not supported yet // TODO
            case meta::field_type_kind::row_reference: return static_cast<hash_value>(-1); // not supported yet // TODO
            case meta::field_type_kind::row_id: return static_cast<hash_value>(-1); // not supported yet // TODO
            case meta::field_type_kind::declared: return static_cast<hash_value>(-1); // not supported yet // TODO
            case meta::field_type_kind::extension: return static_cast<hash_value>(-1); // not supported yet // TODO

            case meta::field_type_kind::reference_column_position: return static_cast<hash_value>(-1); // internal field should be ignored
            case meta::field_type_kind::reference_column_name: return static_cast<hash_value>(-1); // internal field should be ignored
            case meta::field_type_kind::pointer: return static_cast<hash_value>(-1); // internal field should be ignored
        }
        std::abort();
    }
};

}
