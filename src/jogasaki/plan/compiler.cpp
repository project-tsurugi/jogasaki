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
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/aggregate/step.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/plan/parameter_set.h>

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

std::shared_ptr<plan::prepared_statement> prepare(std::string_view sql) {
    shakujo::parser::Parser parser{};
    std::unique_ptr<shakujo::model::program::Program> program{};
    try {
        std::stringstream ss{std::string(sql)};
        program = parser.parse_program("compiler_test", ss);
        // TODO analyze for error check
    } catch (shakujo::parser::Parser::Exception &e) {
        LOG(ERROR) << "parse error:" << e.message() << " (" << e.region() << ")";
    }
    return std::make_shared<plan::prepared_statement>(std::move(program));
};

executor::process::step create(
    takatori::plan::process const& process,
    yugawara::compiled_info const& c_info
) {
    auto info = std::make_shared<executor::process::processor_info>(
        const_cast<relation::graph_type&>(process.operators()),
        c_info
    );

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

executor::exchange::forward::step create(
    takatori::plan::forward const& forward,
    yugawara::compiled_info const& c_info
) {
    meta::variable_order column_order{
        meta::variable_ordering_enum_tag<meta::variable_ordering_kind::flat_record>,
        forward.columns(),
    };
    std::vector<meta::field_type> fields{};
    auto cnt = forward.columns().size();
    fields.reserve(cnt);
    for(auto&& c: forward.columns()) {
        fields.emplace_back(utils::type_for(c_info, c));
    }
    auto meta = std::make_shared<meta::record_meta>(
        std::move(fields),
        boost::dynamic_bitset{cnt}.flip() // currently assuming all fields are nullable
    );
    return executor::exchange::forward::step(
        std::move(meta),
        std::move(column_order));
}

executor::exchange::group::step create(
    takatori::plan::group const& group,
    yugawara::compiled_info const& c_info
) {
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
    auto sz = group.columns().size();
    fields.reserve(sz);
    for(auto&& c: input_order) {
        fields.emplace_back(utils::type_for(c_info, c));
    }
    std::vector<std::size_t> key_indices{};
    key_indices.resize(group.group_keys().size());
    for(auto&& k : group.group_keys()) {
        key_indices[output_order.index(k)] = input_order.index(k);
    }

    std::vector<std::size_t> sort_key_indices{};
    std::vector<executor::ordering> sort_ordering{};
    auto ssz = group.sort_keys().size();
    sort_key_indices.reserve(ssz);
    sort_ordering.reserve(ssz);
    for(auto&& k : group.sort_keys()) {
        auto&& v = k.variable();
        auto ord = k.direction() == takatori::relation::sort_direction::ascendant ?
            executor::ordering::ascending :
            executor::ordering::descending;
        sort_key_indices.emplace_back(input_order.index(v));
        sort_ordering.emplace_back(ord);
    }
    return executor::exchange::group::step(
        std::make_shared<executor::exchange::group::group_info>(
            std::make_shared<meta::record_meta>(
                std::move(fields),
                boost::dynamic_bitset{sz}.flip() // currently assuming all fields are nullable
            ),
            std::move(key_indices),
            std::move(sort_key_indices),
            std::move(sort_ordering)
        ),
        std::move(input_order),
        std::move(output_order)
    );
}

executor::exchange::aggregate::step create(
    takatori::plan::aggregate const& agg,
    yugawara::compiled_info const& c_info
) {
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
        fields.emplace_back(utils::type_for(c_info, c));
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

    std::vector<aggregate_info::value_spec> specs{};
    auto& repo = global::incremental_aggregate_function_repository();
    for(auto&& e : agg.aggregations()) {
        std::vector<std::size_t> argument_indices{};
        for(auto& f : e.arguments()) {
            auto idx = input_order.index(f);
            argument_indices.emplace_back(idx);
        }
        auto& decl = yugawara::binding::extract<yugawara::aggregate::declaration>(e.function());
        auto f = repo.find(decl.definition_id());
        BOOST_ASSERT(f != nullptr);  //NOLINT
        specs.emplace_back(
            *f,
            argument_indices,
            utils::type_for(c_info, e.destination())
        );
    }
    return executor::exchange::aggregate::step(
        std::make_shared<aggregate_info>(
            std::move(meta),
            std::move(key_indices),
            std::move(specs),
            agg.mode() == takatori::plan::group_mode::equivalence_or_whole
        ),
        std::move(input_order),
        std::move(output_order));
}

void create_mirror_for_write(
    compiler_context& ctx,
    unique_object_ptr<statement::statement> statement,
    yugawara::compiled_info info
) {
    auto& node = unsafe_downcast<statement::write>(*statement);
    auto& index = yugawara::binding::extract<yugawara::storage::index>(node.destination());
    auto write = std::make_shared<executor::common::write>(
        executor::process::impl::ops::write_kind_from(node.operator_kind()),
        index.simple_name(),
        index,
        node.columns(),
        node.tuples(),
        *ctx.resource(),
        info
    );
    BOOST_ASSERT( //NOLINT
        node.operator_kind() == relation::write_kind::insert ||
            node.operator_kind() == relation::write_kind::insert_or_update
    );
    ctx.executable_statement(
        std::make_shared<executable_statement>(
            std::move(statement),
            std::move(info),
            std::move(write)
        )
    );
}

void create_mirror_for_execute(
    compiler_context& ctx,
    unique_object_ptr<statement::statement> statement,
    yugawara::compiled_info info
) {
    std::unordered_map<takatori::plan::step const*, executor::common::step*> steps{};
    yugawara::binding::factory bindings{};
    auto mirror = std::make_shared<executor::common::graph>();
    takatori::plan::sort_from_upstream(
        unsafe_downcast<takatori::statement::execute>(*statement).execution_plan(),
        [&mirror, &info, &bindings, &steps](takatori::plan::step const& s){
            switch(s.kind()) {
                case takatori::plan::step_kind::forward: {
                    auto& forward = unsafe_downcast<takatori::plan::forward const>(s);  //NOLINT
                    auto* step = &mirror->emplace<executor::exchange::forward::step>(create(forward, info));
                    auto relation_desc = bindings(forward);
                    steps[&forward] = step;
                    break;
                }
                case takatori::plan::step_kind::group: {
                    auto& group = unsafe_downcast<takatori::plan::group const>(s);  //NOLINT
                    auto* step = &mirror->emplace<executor::exchange::group::step>(create(group, info));
                    auto relation_desc = bindings(group);
                    steps[&group] = step;
                    break;
                }
                case takatori::plan::step_kind::aggregate: {
                    auto& agg = unsafe_downcast<takatori::plan::aggregate const>(s);  //NOLINT
                    auto* step = &mirror->emplace<executor::exchange::aggregate::step>(create(agg, info));
                    auto relation_desc = bindings(agg);
                    steps[&agg] = step;
                    break;
                }
                case takatori::plan::step_kind::broadcast:
                    // TODO implement
                    fail();
                    break;
                case takatori::plan::step_kind::discard:
                    fail();
                    break;
                case takatori::plan::step_kind::process: {
                    auto& process = unsafe_downcast<takatori::plan::process const>(s);  //NOLINT
                    steps[&process] = &mirror->emplace<executor::process::step>(create(process, info));
                    break;
                }
                default:
                    break;
            }
        }
    );

    for(auto&& [s, step] : steps) {
        auto map = std::make_shared<executor::process::io_exchange_map>();
        if(takatori::plan::has_upstream(*s)) {
            takatori::plan::enumerate_upstream(
                *s,
                [step=step, &steps, &map](takatori::plan::step const& up){
                    // assuming enumerate_upstream respects the input port ordering TODO confirm
                    *step << *steps[&up];
                    if(step->kind() == executor::common::step_kind::process) {
                        map->add_input(unsafe_downcast<executor::exchange::step>(steps[&up]));
                    }
                }
            );
        }
        if(takatori::plan::has_downstream(*s)) {
            takatori::plan::enumerate_downstream(
                *s,
                [step=step, &steps, &map](takatori::plan::step const& down){
                    if(step->kind() == executor::common::step_kind::process) {
                        map->add_output(unsafe_downcast<executor::exchange::step>(steps[&down]));
                    }
                }
            );
        }
        if(step->kind() == executor::common::step_kind::process) {
            unsafe_downcast<executor::process::step>(step)->io_exchange_map(std::move(map));
        }
    }
    ctx.executable_statement(std::make_shared<executable_statement>(
        std::move(statement),
        std::move(info),
        std::make_shared<executor::common::execute>(mirror))
    );
}

/**
 * @brief compile prepared statement, resolve parameters, and generate executable statement
 * @pre storage provider exists and populated in the compiler context
 */
status create_executable_statement(compiler_context& ctx, parameter_set const* parameters) {
    auto p = ctx.prepared_statement();
    if (!p) {
        return status::err_invalid_argument;
    }

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
        yugawara::runtime_feature::index_join,
//        yugawara::runtime_feature::broadcast_join_scan,
    };
    std::shared_ptr<yugawara::analyzer::index_estimator> indices {};

    yugawara::compiler_options c_options{
        indices,
        runtime_features,
        options.get_object_creator(),
    };

    auto& placeholders = parameters != nullptr ? parameters->map() : placeholder_map{};

    ::takatori::document::document_map documents;
    auto r = translator(options, *p->program()->main(), documents, placeholders);
    if (! r) {
        auto errors = r.release<result_kind::diagnostics>();
        for(auto&& e : errors) {
            LOG(ERROR) << e.code() << " " << e.message();
        }
        return status::err_translator_error;
    }
    switch(r.kind()) {
        case result_kind::execution_plan: {
            auto ptr = r.release<result_kind::execution_plan>();
            auto&& graph = *ptr;
            auto result = yugawara::compiler()(c_options, std::move(graph));
            if(!result.success()) {
                for (auto&& d : result.diagnostics()) {
                    LOG(ERROR) << "compile result: " << d.code() << " " << d.message() << " at " << d.location();
                }
                return status::err_compiler_error;
            }
            create_mirror_for_execute(
                ctx,
                result.release_statement(),
                result.info()
            );
            break;
        }
        case result_kind::statement: {
            auto ptr = r.release<result_kind::statement>();
            auto&& stmt = *ptr;
            auto result = yugawara::compiler()(c_options, std::move(stmt));
            if(! result.success()) {
                for (auto&& d : result.diagnostics()) {
                    LOG(ERROR) << "compile result: " << d.code() << " " << d.message() << " at " << d.location();
                }
                return status::err_compiler_error;
            }
            create_mirror_for_write(
                ctx,
                result.release_statement(),
                result.info()
            );
            break;
        }
        default:
            fail();
    }
    return status::ok;
}

} // namespace impl

status prepare(std::string_view sql, compiler_context &ctx) {
    if(auto p = impl::prepare(sql); p != nullptr && p->program()) {
        ctx.prepared_statement(std::move(p));
        return status::ok;
    }
    return status::err_parse_error;
}

status compile(
    compiler_context &ctx,
    parameter_set const* parameters
) {
    if(auto rc = impl::create_executable_statement(ctx, parameters); rc != status::ok) {
        return rc;
    }
    return status::ok;
}

status compile(
    std::string_view sql,
    compiler_context &ctx,
    parameter_set const* parameters
) {
    if(auto rc = prepare(sql, ctx); rc != status::ok) {
        return rc;
    }
    if(auto rc = compile(ctx, parameters); rc != status::ok) {
        return rc;
    }
    return status::ok;
}

}