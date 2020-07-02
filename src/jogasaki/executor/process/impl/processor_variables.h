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

#include <cassert>

#include <takatori/relation/expression.h>
#include <takatori/util/fail.h>
#include <yugawara/analyzer/block_builder.h>
#include <yugawara/analyzer/block_algorithm.h>
#include <yugawara/analyzer/variable_liveness_analyzer.h>
#include <jogasaki/data/small_record_store.h>
#include "block_variables.h"

namespace jogasaki::executor::process::impl {

/**
 * @brief variables data regions used in a processor
 */
class processor_variables {
public:
    processor_variables() = default;

    std::vector<meta::field_type> target_fields(yugawara::analyzer::block blk) {
        auto& back = blk.back();
        if (back.kind() != takatori::relation::expression_kind::buffer) {

        }
        return {};
    }

    explicit processor_variables(takatori::graph::graph<takatori::relation::expression>& operators,
        memory::paged_memory_resource* resource = nullptr) {
        // analyze liveness
        // for each basic block, define a block_variables region with
        // result fields + defined fields (except killed in the same basic block)
        auto bg = yugawara::analyzer::block_builder::build(operators);
        yugawara::analyzer::variable_liveness_analyzer analyzer { bg };

        // FIXME support multiple blocks
        auto b0 = find_unique_head(bg);
        if (!b0) {
            // TODO multiple heads supported?
            takatori::util::fail();
        }
        auto&& n0 = analyzer.inspect(*b0);

        auto& killed = n0.kill();
        std::unordered_map<takatori::descriptor::variable, value_info> map{};
        std::vector<meta::field_type> fields{};
        // TODO add target meta
        fields.reserve(n0.define().size());
        std::vector<takatori::descriptor::variable> variables{};
        for(auto& v : n0.define()) {
            if (killed.count(v) == 0) {
                fields.emplace_back(meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int4>)); // TODO fetch type
                map[v] = value_info{};
                variables.emplace_back(v);
            }
        }
        boost::dynamic_bitset<std::uint64_t> nullability{};
        nullability.resize(fields.size()); // TODO fetch nullability
        auto meta = std::make_shared<meta::record_meta>(std::move(fields), std::move(nullability));
        assert(meta->field_count() == variables.size());
        for(std::size_t i=0, n = meta->field_count(); i < n; ++i) {
            auto& v = variables[i];
            map[v] = value_info{meta->value_offset(i), meta->nullity_offset(i)};
        }
        block_variables_.emplace_back(
            std::make_unique<data::small_record_store>(meta, 1, resource),
            std::make_unique<variable_value_map>(std::move(map)),
            meta);
    }

    [[nodiscard]] std::vector<class block_variables> const& block_variables() const noexcept {
        return block_variables_;
    }

private:
    std::vector<class block_variables> block_variables_{};
};

}


