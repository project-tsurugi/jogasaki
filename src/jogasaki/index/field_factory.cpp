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
#include "field_factory.h"

#include <cstddef>
#include <type_traits>

#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/index/utils.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::index {

std::vector<field_info> index_fields(
    yugawara::storage::index const& idx,
    bool key
) {
    std::vector<field_info> ret{};
    yugawara::binding::factory bindings{};
    if (key) {
        auto meta = create_meta(idx, true);
        ret.reserve(idx.keys().size());
        for(std::size_t i=0, n=idx.keys().size(); i<n; ++i) {
            auto&& k = idx.keys()[i];
            auto kc = bindings(k.column());
            auto t = utils::type_for(k.column().type());
            auto spec = k.direction() == takatori::relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            ret.emplace_back(
                t,
                true,
                meta->value_offset(i),
                meta->nullity_offset(i),
                k.column().criteria().nullity().nullable(),
                spec
            );
        }
        return ret;
    }
    auto meta = create_meta(idx, false);
    ret.reserve(idx.values().size());
    for(std::size_t i=0, n=idx.values().size(); i<n; ++i) {
        auto&& v = idx.values()[i];
        auto b = bindings(v);
        auto& c = static_cast<yugawara::storage::column const&>(v);
        auto t = utils::type_for(c.type());
        auto spec = kvs::spec_value;
        ret.emplace_back(
            t,
            true,
            meta->value_offset(i),
            meta->nullity_offset(i),
            c.criteria().nullity().nullable(),
            spec
        );
    }
    return ret;
}

}


