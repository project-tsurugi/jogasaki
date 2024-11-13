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

#include <yugawara/storage/index.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/process/processor_info.h>

namespace jogasaki::executor::process::impl::ops::details {

/**
 * @brief key field info of the join_find operation
 * @details join_find operator uses these fields to know how to create search key sequence from the variables
 */
struct cache_align search_key_field_info {
    /**
     * @brief create new object
     * @param type type of the key field
     * @param nullable whether the target field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     * @param evaluator evaluator used to evaluate the key field value
     */
    search_key_field_info(
        meta::field_type type,
        bool nullable,
        kvs::coding_spec spec,
        expr::evaluator evaluator
    ) :
        type_(std::move(type)),
        nullable_(nullable),
        spec_(spec),
        evaluator_(evaluator)
    {}

    meta::field_type type_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
    expr::evaluator evaluator_{}; //NOLINT
};

template<class Key>
std::vector<details::search_key_field_info> create_search_key_fields(
    yugawara::storage::index const& primary_or_secondary_idx,
    takatori::tree::tree_fragment_vector<Key> const& keys,
    processor_info const& info
) {
    if (keys.empty()) {
        return {};
    }
    BOOST_ASSERT(keys.size() <= primary_or_secondary_idx.keys().size());  //NOLINT // possibly partial keys
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};

    std::unordered_map<variable, takatori::scalar::expression const*> var_to_expression{};
    for(auto&& k : keys) {
        var_to_expression.emplace(k.variable(), &k.value());
    }

    std::vector<details::search_key_field_info> ret{};
    ret.reserve(primary_or_secondary_idx.keys().size());
    for(auto&& k : primary_or_secondary_idx.keys()) {
        auto kc = bindings(k.column());
        auto t = utils::type_for(k.column().type());
        auto spec = k.direction() == relation::sort_direction::ascendant ?
            kvs::spec_key_ascending : kvs::spec_key_descending; // no storage spec with fields for read
        if (var_to_expression.count(kc) == 0) {
            continue;
        }
        auto* exp = var_to_expression.at(kc);
        ret.emplace_back(
            t,
            k.column().criteria().nullity().nullable(),
            spec,
            expr::evaluator{*exp, info.compiled_info(), info.host_variables()}
        );
    }
    return ret;
}

}  // namespace jogasaki::executor::process::impl::ops::details
