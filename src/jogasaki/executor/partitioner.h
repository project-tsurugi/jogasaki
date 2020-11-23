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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

/**
 * @brief partitioner determines input partition for the record to be sent
 */
class partitioner {
public:
    /**
     * @brief construct empty object
     */
    partitioner() = default;

    /**
     * @brief construct new object
     * @param partitions number of total partitions
     * @param meta schema information of the record whose target is calculated by this partitioner
     */
    partitioner(std::size_t partitions, maybe_shared_ptr<meta::record_meta> meta) noexcept :
            partitions_(partitions), meta_(std::move(meta)) {}

    /**
     * @brief retrieve the partition for the given record
     * @param key the record to be evaluated
     * @return the target partition number
     */
    [[nodiscard]] std::size_t operator()(accessor::record_ref key) const noexcept {
        static const std::size_t p = 18446744073709551557ULL; // arbitrary prime in int64_t
        std::size_t h = 0;
        for(std::size_t i = 0, n = meta_->field_count(); i < n; ++i) {
            h += field_hash(key, i);
            h *= i == 0 ? 1 : p;
        }
        return h % partitions_;
    }

private:
    std::size_t partitions_{};
    maybe_shared_ptr<meta::record_meta> meta_{};

    template <meta::field_type_kind Kind>
    using runtime_type = typename meta::field_type_traits<Kind>::runtime_type;

    using kind = meta::field_type_kind;

    [[nodiscard]] std::size_t field_hash(accessor::record_ref key, std::size_t field_index) const {
        auto type = meta_->at(field_index);
        auto offset = meta_->value_offset(field_index);
        switch(type.kind()) {
            case meta::field_type_kind::boolean: return std::hash<runtime_type<kind::boolean>>()(key.get_value<runtime_type<kind::boolean>>(offset));
            case meta::field_type_kind::int1: return std::hash<runtime_type<kind::int1>>()(key.get_value<runtime_type<kind::int1>>(offset));
            case meta::field_type_kind::int2: return std::hash<runtime_type<kind::int2>>()(key.get_value<runtime_type<kind::int2>>(offset));
            case meta::field_type_kind::int4: return std::hash<runtime_type<kind::int4>>()(key.get_value<runtime_type<kind::int4>>(offset));
            case meta::field_type_kind::int8: return std::hash<runtime_type<kind::int8>>()(key.get_value<runtime_type<kind::int8>>(offset));
            case meta::field_type_kind::float4: return std::hash<runtime_type<kind::float4>>()(key.get_value<runtime_type<kind::float4>>(offset));
            case meta::field_type_kind::float8: return std::hash<runtime_type<kind::float8>>()(key.get_value<runtime_type<kind::float8>>(offset));
            case meta::field_type_kind::character: return std::hash<runtime_type<kind::character>>()(key.get_value<runtime_type<kind::character>>(offset));
            default:
                // TODO implement other types
                std::abort();
        }
    }
};

}
