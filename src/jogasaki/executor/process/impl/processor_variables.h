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
#include <takatori/relation/emit.h>
#include <takatori/relation/step/offer.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/field_types.h>
#include "block_variables.h"

namespace jogasaki::executor::process::impl {

/**
 * @brief variables data regions used in a processor
 */
class processor_variables {
public:
    processor_variables() = default;

    explicit processor_variables(
        takatori::graph::graph<takatori::relation::expression>& operators,
        yugawara::compiled_info& info,
        memory::paged_memory_resource* resource = nullptr) : block_variables_(create_block_variables(operators, info, resource))
    {}

    [[nodiscard]] std::vector<class block_variables> const& block_variables() const noexcept {
        return block_variables_;
    }

private:
    std::vector<class block_variables> block_variables_{};

    void process_target_fields(yugawara::analyzer::block const& blk,
        yugawara::compiled_info& info,
        std::vector<meta::field_type>& fields,
        std::vector<takatori::descriptor::variable>& variables,
        std::unordered_map<takatori::descriptor::variable, value_info>& map
    ) {
        auto& back = blk.back();
        switch(back.kind()) {
            case takatori::relation::expression_kind::buffer:
                return;
            case takatori::relation::expression_kind::emit: {
                auto& emit = static_cast<takatori::relation::emit const&>(back);
                for(auto &c : emit.columns()) {
                    auto& v = c.source();
                    variables.emplace_back(v);
                    map[v] = value_info{};
                    fields.emplace_back(utils::type_for(info, v));
                }
                break;
            }
            case takatori::relation::expression_kind::offer: {
                auto& offer = static_cast<takatori::relation::step::offer const&>(back);
                for(auto &c : offer.columns()) {
                    auto& v = c.destination();
                    variables.emplace_back(v);
                    map[v] = value_info{};
                    fields.emplace_back(utils::type_for(info, v));
                }
                break;
            }
            default:
                takatori::util::fail();
        }
    }
    std::vector<class block_variables> create_block_variables(
        takatori::graph::graph<takatori::relation::expression>& operators,
        yugawara::compiled_info& info,
        memory::paged_memory_resource* resource) {

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
        std::vector<takatori::descriptor::variable> variables{};
        process_target_fields(*b0, info, fields, variables, map);

        fields.reserve(n0.define().size());
        for(auto& v : n0.define()) {
            if (killed.count(v) == 0) {
                fields.emplace_back(utils::type_for(info, v));
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
        std::vector<class block_variables> ret{};
        ret.emplace_back(
            std::make_unique<data::small_record_store>(meta, 1, resource),
            std::make_unique<variable_value_map>(std::move(map)),
            meta);
        return ret;
    }
};

}

