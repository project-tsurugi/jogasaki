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
#include "compiler.h"

#include <cstddef>
#include <functional>

#include <glog/logging.h>

#include <shakujo/parser/Parser.h>
#include <shakujo/common/core/Type.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/binding/factory.h>
#include <yugawara/binding/extract.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>

#include <mizugaki/translator/shakujo_translator.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/statement/write.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/plan/group.h>
#include <takatori/plan/aggregate.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/forward.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/builtin_functions.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/executor/exchange/aggregate/step.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include "compiler_context.h"

namespace jogasaki::plan {

///@private
namespace impl {

using namespace takatori::util;

using namespace ::mizugaki::translator;
using namespace ::mizugaki;

using code = shakujo_translator_code;
using result_kind = shakujo_translator::result_type::kind_type;
namespace statement = ::takatori::statement;

namespace relation = takatori::relation;

using takatori::util::unsafe_downcast;

std::unique_ptr<shakujo::model::program::Program> generate_program(std::string_view sql) {
    shakujo::parser::Parser parser{};
    std::unique_ptr<shakujo::model::program::Program> program{};
    try {
        std::stringstream ss{std::string(sql)};
        program = parser.parse_program("compiler_test", ss);
        // TODO analyze for error check
    } catch (shakujo::parser::Parser::Exception &e) {
        LOG(ERROR) << "parse error:" << e.message() << " (" << e.region() << ")";
    }
    return program;
};

/**
 * @brief compile sql and generate takatori step graph
 * @pre storage provider exists and populated in the compiler context
 */
bool create_step_graph(std::string_view sql, compiler_context& ctx) {
    shakujo_translator translator;
    shakujo_translator_options options {
        ctx.storage_provider(),
        ctx.variable_provider(),
        ctx.function_provider(),
        ctx.aggregate_provider()
    };

    yugawara::runtime_feature_set runtime_features {
        //TODO enable features
//        yugawara::runtime_feature::broadcast_exchange,
        yugawara::runtime_feature::aggregate_exchange,
//        yugawara::runtime_feature::index_join,
//        yugawara::runtime_feature::broadcast_join_scan,
    };
    std::shared_ptr<yugawara::analyzer::index_estimator> indices {};

    yugawara::compiler_options c_options{
        indices,
        runtime_features,
        options.get_object_creator(),
    };

    placeholder_map placeholders;
    ::takatori::document::document_map documents;
    auto p = generate_program(sql);
    if (!p) {
        return false;
    }
    auto r = translator(options, *p->main(), documents, placeholders);
    if (! r) {
        auto errors = r.release<result_kind::diagnostics>();
        for(auto&& e : errors) {
            LOG(ERROR) << e.code() << " " << e.message();
        }
        return false;
    }
    ::yugawara::compiler_result result{};
    if (r.kind() == result_kind::execution_plan) {
        auto ptr = r.release<result_kind::execution_plan>();
        auto&& graph = *ptr;
        result = yugawara::compiler()(c_options, std::move(graph));
    }
    if (r.kind() == result_kind::statement) {
        auto ptr = r.release<result_kind::statement>();
        auto&& stmt = *ptr;
        result = yugawara::compiler()(c_options, std::move(stmt));
    }
    if(!result.success()) {
        return false;
    }
    ctx.compiler_result(std::move(result));
    return true;
}

executor::process::step create(takatori::plan::process const& process, compiler_context& ctx) {
    auto info = std::make_shared<executor::process::processor_info>(
        const_cast<relation::graph_type&>(process.operators()), ctx.compiled_info());

    yugawara::binding::factory bindings{};
    std::unordered_map<takatori::descriptor::relation, std::size_t> inputs{};
    std::unordered_map<takatori::descriptor::relation, std::size_t> outputs{};
    auto upstreams = process.upstreams();
    for(std::size_t i=0, n=upstreams.size(); i < n; ++i) {
        inputs[bindings(upstreams[i])] = i;
    }
    auto downstreams = process.downstreams();
    for(std::size_t i=0, n=downstreams.size(); i < n; ++i) {
        outputs[bindings(downstreams[i])] = i;
    }
    return executor::process::step(
        std::move(info),
        std::make_shared<executor::process::relation_io_map>(std::move(inputs), std::move(outputs))
    );
}

executor::exchange::forward::step create(takatori::plan::forward const& forward, compiler_context &ctx) {
    meta::variable_order column_order{
        meta::variable_ordering_enum_tag<meta::variable_ordering_kind::flat_record>,
        forward.columns(),
    };
    std::vector<meta::field_type> fields{};
    auto cnt = forward.columns().size();
    fields.reserve(cnt);
    for(auto&& c: forward.columns()) {
        fields.emplace_back(utils::type_for(ctx.compiled_info(), c));
    }
    auto meta = std::make_shared<meta::record_meta>(
        std::move(fields),
        boost::dynamic_bitset{cnt}.flip() // currently assuming all fields are nullable
    );
    return executor::exchange::forward::step(
        std::move(meta),
        std::move(column_order));
}

executor::exchange::group::step create(takatori::plan::group const& group, compiler_context& ctx) {
    meta::variable_order input_order{
        meta::variable_ordering_enum_tag<meta::variable_ordering_kind::flat_record>,
        group.columns(),
    };
    meta::variable_order output_order{
        meta::variable_ordering_enum_tag<meta::variable_ordering_kind::group_from_keys>,
        group.columns(),
        group.group_keys()
    };

    std::vector<meta::field_type> fields{};
    for(auto&& c: group.columns()) {
        fields.emplace_back(utils::type_for(ctx.compiled_info(), c));
    }
    auto cnt = fields.size();
    auto meta = std::make_shared<meta::record_meta>(
        std::move(fields),
        boost::dynamic_bitset{cnt}.flip() // currently assuming all fields are nullable
    );
    std::vector<std::size_t> key_indices{};
    key_indices.resize(group.group_keys().size());
    for(auto&& k : group.group_keys()) {
        key_indices[output_order.index(k)] = input_order.index(k);
    }

    auto info = std::make_shared<executor::exchange::group::shuffle_info>(std::move(meta), std::move(key_indices));
    return executor::exchange::group::step(
        std::move(info),
        std::move(input_order),
        std::move(output_order));
}

executor::exchange::aggregate::step create(takatori::plan::aggregate const& agg, compiler_context& ctx) {
    using executor::exchange::aggregate::aggregate_info;
    meta::variable_order input_order{
        meta::variable_ordering_enum_tag<meta::variable_ordering_kind::flat_record>,
        agg.source_columns(),
    };
    meta::variable_order output_order{
        meta::variable_ordering_enum_tag<meta::variable_ordering_kind::group_from_keys>,
        agg.destination_columns(),
        agg.group_keys()
    };

    std::vector<meta::field_type> fields{};
    for(auto&& c: agg.source_columns()) {
        fields.emplace_back(utils::type_for(ctx.compiled_info(), c));
    }
    auto sz = fields.size();
    auto meta = std::make_shared<meta::record_meta>(
        std::move(fields),
        boost::dynamic_bitset{sz}.flip() // currently assuming all fields are nullable
    );
    std::vector<std::size_t> key_indices{};
    key_indices.resize(agg.group_keys().size());
    for(auto&& k : agg.group_keys()) {
        key_indices[output_order.index(k)] = input_order.index(k);
    }

    std::vector<aggregate_info::value_spec> mid_specs{};
    std::vector<aggregate_info::value_spec> post_specs{};
    for(auto&& e : agg.aggregations()) {
        std::vector<std::size_t> argument_indices{};
        for(auto& f : e.arguments()) {
            auto idx = input_order.index(f);
            argument_indices.emplace_back(idx);
        }

        auto& provider = ctx.aggregate_provider();
        auto& decl = yugawara::binding::extract<yugawara::aggregate::declaration>(e.function());
        (void)decl;
        (void)provider;
        mid_specs.emplace_back(
            executor::builtin::sum, //FIXME search provider
            (argument_indices),
            utils::type_for(ctx.compiled_info(), e.destination())
        );
        post_specs.emplace_back(
            executor::builtin::sum, //FIXME search provider
            (argument_indices),
            utils::type_for(ctx.compiled_info(), e.destination())
        );
    }
    auto info = std::make_shared<aggregate_info>(
        std::move(meta),
        std::move(key_indices),
        std::move(mid_specs),
        std::move(post_specs)
    );
    return executor::exchange::aggregate::step(
        std::move(info),
        std::move(input_order),
        std::move(output_order));
}

void create_mirror_for_execute(compiler_context& ctx) {
    std::unordered_map<takatori::plan::step const*, executor::common::step*> steps{};
    yugawara::binding::factory bindings{};
    auto&& c = downcast<statement::execute>(ctx.statement());
    auto mirror = std::make_shared<executor::common::graph>();
    takatori::plan::sort_from_upstream(c.execution_plan(), [&mirror, &ctx, &bindings, &steps](takatori::plan::step const& s){
        switch(s.kind()) {
            case takatori::plan::step_kind::forward: {
                auto& forward = unsafe_downcast<takatori::plan::forward const>(s);  //NOLINT
                auto* step = &mirror->emplace<executor::exchange::forward::step>(create(forward, ctx));
                auto relation_desc = bindings(forward);
                steps[&forward] = step;
                break;
            }
            case takatori::plan::step_kind::group: {
                auto& group = unsafe_downcast<takatori::plan::group const>(s);  //NOLINT
                auto* step = &mirror->emplace<executor::exchange::group::step>(create(group, ctx));
                auto relation_desc = bindings(group);
                steps[&group] = step;
                break;
            }
            case takatori::plan::step_kind::aggregate: {
                auto& agg = unsafe_downcast<takatori::plan::aggregate const>(s);  //NOLINT
                auto* step = &mirror->emplace<executor::exchange::aggregate::step>(create(agg, ctx));
                auto relation_desc = bindings(agg);
                steps[&agg] = step;
                break;
            }
            case takatori::plan::step_kind::broadcast:
                // TODO implement
                break;
            case takatori::plan::step_kind::discard:
                break;
            case takatori::plan::step_kind::process: {
                auto& process = unsafe_downcast<takatori::plan::process const>(s);  //NOLINT
                steps[&process] = &mirror->emplace<executor::process::step>(create(process, ctx));
                break;
            }
            default:
                break;
        }
    });
    for(auto&& [s, step] : steps) {
        auto map = std::make_shared<executor::process::io_exchange_map>();
        if(takatori::plan::has_upstream(*s)) {
            takatori::plan::enumerate_upstream(*s, [step=step, &steps, &map](takatori::plan::step const& up){
                // assuming enumerate_upstream respects the input port ordering TODO confirm
                *step << *steps[&up];
                if(step->kind() == executor::common::step_kind::process) {
                    map->add_input(unsafe_downcast<executor::exchange::step>(steps[&up]));
                }
            });
        }
        if(takatori::plan::has_downstream(*s)) {
            takatori::plan::enumerate_downstream(*s, [step=step, &steps, &map](takatori::plan::step const& down){
                if(step->kind() == executor::common::step_kind::process) {
                    map->add_output(unsafe_downcast<executor::exchange::step>(steps[&down]));
                }
            });
        }
        if(step->kind() == executor::common::step_kind::process) {
            unsafe_downcast<executor::process::step>(step)->io_exchange_map(std::move(map));
        }
    }
    ctx.step_graph(std::move(mirror));
}

/**
 * @brief create jogasaki step graph based on takatori/yugawara compile result
 * @pre storage provider exists in the compiler context
 * @pre compile result are populated in the compiler context
 */
bool create_mirror(compiler_context& ctx) {
    auto& statement = ctx.statement();
    using statement_kind = takatori::statement::statement_kind;
    switch(statement.kind()) {
        case statement_kind::execute:
            create_mirror_for_execute(ctx);
            break;
        case statement_kind::write: {
            /*
            auto&& c = downcast<statement::write>(statement);
            auto& g = c.();
            auto mirror = std::make_shared<executor::common::graph>();
            auto& process = static_cast<takatori::plan::process const&>(s);  //NOLINT
            steps[&process] = &mirror->emplace<executor::process::step>(create(process, ctx));
            break;
             */
            fail();
        }
        case statement_kind::extension:
            return false;
    }
    return true;
}

} // namespace impl

bool compile(std::string_view sql, compiler_context &ctx) {
    if(auto success = impl::create_step_graph(sql, ctx); !success) {
        return false;
    }
    if(auto success = impl::create_mirror(ctx); !success) {
        return false;
    }
    return true;
}

}