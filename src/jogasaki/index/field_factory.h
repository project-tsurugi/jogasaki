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

#include <takatori/util/sequence_view.h>
#include <yugawara/storage/index.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/index/field_info.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::index {

using takatori::util::sequence_view;
using variable_table_info = executor::process::impl::variable_table_info;

std::vector<field_info> index_fields(
    yugawara::storage::index const& idx,
    bool key
);

template <class Column>
std::vector<index::field_info> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<Column const> columns,
    variable_table_info const& varinfo,
    bool key,
    bool for_output
) {
    std::vector<index::field_info> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, variable> mapping{};
    for(auto&& c : columns) {
        if(for_output) {
            mapping.emplace(c.source(), c.destination());
            continue;
        }
        mapping.emplace(c.destination(), c.source());
    }
    if (key) {
        ret.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto kc = bindings(k.column());
            auto t = utils::type_for(k.column().type());
            auto spec = k.direction() == takatori::relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            if (mapping.count(kc) == 0) {
                ret.emplace_back(
                    t,
                    false,
                    0,
                    0,
                    k.column().criteria().nullity().nullable(),
                    spec
                );
                continue;
            }
            auto&& var = mapping.at(kc);
            ret.emplace_back(
                t,
                true,
                varinfo.at(var).value_offset(),
                varinfo.at(var).nullity_offset(),
                k.column().criteria().nullity().nullable(),
                spec
            );
        }
        return ret;
    }
    ret.reserve(idx.values().size());
    for(auto&& v : idx.values()) {
        auto b = bindings(v);
        auto& c = static_cast<yugawara::storage::column const&>(v);
        auto t = utils::type_for(c.type());
        if (mapping.count(b) == 0) {
            ret.emplace_back(
                t,
                false,
                0,
                0,
                c.criteria().nullity().nullable(),
                kvs::spec_value
            );
            continue;
        }
        auto&& var = mapping.at(b);
        ret.emplace_back(
            t,
            true,
            varinfo.at(var).value_offset(),
            varinfo.at(var).nullity_offset(),
            c.criteria().nullity().nullable(),
            kvs::spec_value
        );
    }
    return ret;
}
}


