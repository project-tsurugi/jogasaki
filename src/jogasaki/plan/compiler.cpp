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
#include "compiler.h"

#include <cstddef>
#include <functional>

#include <glog/logging.h>

#include <shakujo/parser/Parser.h>
#include <shakujo/analyzer/SyntaxValidator.h>
#include <shakujo/common/core/Type.h>

#include <yugawara/binding/factory.h>
#include <yugawara/binding/extract.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_result.h>
#include <yugawara/compiled_info.h>
#include <yugawara/compiler_options.h>
#include <yugawara/analyzer/variable_resolution.h>

#include <mizugaki/translator/shakujo_translator.h>

#include <takatori/statement/statement_kind.h>
#include <takatori/statement/write.h>
#include <takatori/statement/create_table.h>
#include <takatori/statement/drop_table.h>
#include <takatori/statement/create_index.h>
#include <takatori/statement/drop_index.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/plan/group.h>
#include <takatori/plan/aggregate.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/forward.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/find.h>
#include <takatori/util/downcast.h>
#include <takatori/util/exception.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/common/graph.h>
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
#include <jogasaki/executor/common/write.h>
#include <jogasaki/executor/process/impl/ops/emit.h>
#include <jogasaki/executor/common/create_table.h>
#include <jogasaki/executor/common/drop_table.h>
#include <jogasaki/executor/common/create_index.h>
#include <jogasaki/executor/common/drop_index.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/plan/parameter_set.h>
#include <jogasaki/model/statement_kind.h>
#include <jogasaki/plan/storage_processor.h>

namespace jogasaki::plan {

#define set_compile_error(ctx, code, msg, st) jogasaki::plan::impl::set_compile_error_impl(ctx, code, msg, __FILE__, line_number_string, st) //NOLINT

using takatori::util::throw_exception;

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

/**
 * @brief set error info to the compiler context
 * @param ctx compiler context to set error
 * @param info error info to be set
 */
void set_compile_error_impl(
    compiler_context& ctx,
    jogasaki::error_code code,
    std::string_view message,
    std::string_view filepath,
    std::string_view position,
    status st
) {
    auto info = error::create_error_info_impl(code, message, filepath, position, st, false);
    ctx.error_info(info);
}

void preprocess(
    takatori::plan::process const& process,
    compiled_info const& info,
    std::shared_ptr<mirror_container> const& container
) {
    container->set(
        std::addressof(process),
        executor::process::impl::create_block_variables_definition(process.operators(), info)
    );
    takatori::relation::sort_from_upstream(process.operators(), [&container, &info](takatori::relation::expression const& op){
        switch(op.kind()) {
            case takatori::relation::expression_kind::emit: {
                auto& e = unsafe_downcast<takatori::relation::emit>(op);
                container->external_writer_meta(executor::process::impl::ops::emit::create_meta(info, e.columns()));
                container->work_level().set_minimum(statement_work_level_kind::key_operation);
                break;
            }
            case takatori::relation::expression_kind::find: {
                auto& f = unsafe_downcast<takatori::relation::find>(op);

                auto& secondary_or_primary_index = yugawara::binding::extract<yugawara::storage::index>(f.source());
                auto& table = secondary_or_primary_index.table();
                auto primary = table.owner()->find_primary_index(table);
                if(*primary == secondary_or_primary_index) {
                    // find uses primary index
                    container->work_level().set_minimum(statement_work_level_kind::key_operation);
                    break;
                }
                // find uses secondary index
                container->work_level().set_minimum(statement_work_level_kind::simple_crud);
                break;
            }
            case takatori::relation::expression_kind::values: //fall-thru
            case takatori::relation::expression_kind::write: //fall-thru
                container->work_level().set_minimum(statement_work_level_kind::key_operation);
                break;
            case takatori::relation::expression_kind::filter: //fall-thru
            case takatori::relation::expression_kind::project: //fall-thru
            // TODO check if UDF is not used for filter/project
                container->work_level().set_minimum(statement_work_level_kind::simple_crud);
                break;
            case takatori::relation::expression_kind::take_flat: //fall-thru
            case takatori::relation::expression_kind::offer: //fall-thru
                container->work_level().set_minimum(statement_work_level_kind::simple_multirecord_operation);
                break;
            case takatori::relation::expression_kind::join_scan:
                throw_exception(impl::exception(
                    create_error_info(
                        error_code::unsupported_runtime_feature_exception,
                        "Compiling statement resulted in unsupported relational operator. "
                        "Specify configuration parameter enable_index_join=false to avoid this.",
                        status::err_unsupported
                    )
                ));
            case takatori::relation::expression_kind::join_find: //fall-thru
            case takatori::relation::expression_kind::join_group: //fall-thru
            case takatori::relation::expression_kind::take_group: //fall-thru
            case takatori::relation::expression_kind::take_cogroup: //fall-thru
                // TODO check if UDF is not used
                container->work_level().set_minimum(statement_work_level_kind::join);
                break;
            case takatori::relation::expression_kind::aggregate_group:
                container->work_level().set_minimum(statement_work_level_kind::aggregate);
                break;
            case takatori::relation::expression_kind::scan:
                container->work_level().set_minimum(statement_work_level_kind::infinity);
                break;
            default:
                break;
    }
    });
}

std::shared_ptr<mirror_container> preprocess_mirror(
    maybe_shared_ptr<statement::statement> const& statement,
    std::shared_ptr<::yugawara::variable::configurable_provider> const& provider,
    compiled_info info
) {
    auto container = std::make_shared<mirror_container>();
    switch(statement->kind()) {
        case statement::statement_kind::execute:
            container->work_level().set_minimum(statement_work_level_kind::key_operation);
            takatori::plan::sort_from_upstream(
                unsafe_downcast<takatori::statement::execute>(*statement).execution_plan(),
                [&container, &info](takatori::plan::step const& s){
                    switch(s.kind()) {
                        case takatori::plan::step_kind::process: {
                            auto& process = unsafe_downcast<takatori::plan::process const>(s);  //NOLINT
                            preprocess(process, info, container);
                            break;
                        }
                        case takatori::plan::step_kind::group:
                            container->work_level().set_minimum(statement_work_level_kind::join);
                            break;
                        case takatori::plan::step_kind::aggregate:
                            // TODO check if UDF is not used
                            container->work_level().set_minimum(statement_work_level_kind::aggregate);
                            break;
                        case takatori::plan::step_kind::forward:
                            // TODO check if UDF is not used
                            container->work_level().set_minimum(statement_work_level_kind::simple_multirecord_operation);
                            break;
                        default:
                            break;
                    }
                }
            );
            break;
        case statement::statement_kind::write:
            container->work_level().set_minimum(statement_work_level_kind::simple_write);
            break;
        case statement::statement_kind::create_table:
            container->work_level().set_minimum(statement_work_level_kind::infinity);
            break;
        case statement::statement_kind::drop_table:
            container->work_level().set_minimum(statement_work_level_kind::infinity);
            break;
        case statement::statement_kind::create_index:
            container->work_level().set_minimum(statement_work_level_kind::infinity);
            break;
        case statement::statement_kind::drop_index:
            container->work_level().set_minimum(statement_work_level_kind::infinity);
            break;
        default:
            throw_exception(std::logic_error{""});
    }
    container->host_variable_info(create_host_variable_info(provider, info));
    return container;
}

template <result_kind Kind>
yugawara::compiler_result compile_internal(
    shakujo_translator::result_type& r,
    yugawara::compiler_options& c_options) {
    auto ptr = r.release<Kind>();
    return yugawara::compiler()(c_options, std::move(*ptr));
}

error_code map_compiler_error(shakujo_translator_code code) {
    using stc = shakujo_translator_code;
    using ec = error_code;
    switch(code) {
        case stc::table_not_found: return ec::symbol_analyze_exception;
        case stc::index_not_found: return ec::symbol_analyze_exception;
        case stc::column_not_found: return ec::symbol_analyze_exception;
        case stc::variable_not_found: return ec::symbol_analyze_exception;
        case stc::function_not_found: return ec::symbol_analyze_exception;
        case stc::unresolved_variable: return ec::symbol_analyze_exception;

        case stc::inconsistent_type: return ec::type_analyze_exception;
        case stc::ambiguous_type: return ec::type_analyze_exception;

        case stc::unsupported_type: return ec::unsupported_compiler_feature_exception;
        case stc::unsupported_value: return ec::unsupported_compiler_feature_exception;
        case stc::unsupported_statement: return ec::unsupported_compiler_feature_exception;
        case stc::unsupported_scalar_expression: return ec::unsupported_compiler_feature_exception;
        case stc::unsupported_relational_operator: return ec::unsupported_compiler_feature_exception;

        default: return ec::compile_exception;
    }
    std::abort();
}

error_code map_compiler_error(yugawara::compiler_code code) {
    using ycc = yugawara::compiler_code;
    using ec = error_code;
    switch(code) {
        case ycc::ambiguous_type: return ec::type_analyze_exception;
        case ycc::inconsistent_type: return ec::type_analyze_exception;
        case ycc::unsupported_type: return ec::unsupported_compiler_feature_exception;
        case ycc::unresolved_variable: return ec::symbol_analyze_exception;
        default: return ec::compile_exception;
    }
    std::abort();
}

template <class T>
void handle_compile_error(
    T&& errors,
    status res,
    compiler_context &ctx
) {
    {
        // logging internal message
        std::stringstream msg{};
        msg << "compile failed. ";
        for(auto&& e : errors) {
            msg << "error:" << e.code() << " message:\"" << e.message() << "\" location:" << e.location() <<" ";
        }
        VLOG_LP(log_error) << msg.str();
    }
    if(errors.empty()) {
        set_compile_error(
            ctx,
            error_code::compile_exception,
            "unknown compile error occurred.",
            res
        );
        return;
    }

    // only the primary error is returned to caller
    auto& err = errors.at(0);
    auto code = map_compiler_error(err.code());
    auto msg =
        string_builder{} << "compile failed with error:" << err.code() << " message:\"" <<
            err.message() << "\" location:" << err.location() << string_builder::to_string;
    set_compile_error(ctx, code, msg, res);
}

status create_prepared_statement(
    shakujo_translator::result_type& r,
    std::shared_ptr<::yugawara::variable::configurable_provider> const& provider,
    yugawara::compiler_options& c_options,
    std::shared_ptr<storage_processor> const& sp,
    compiler_context &ctx,
    std::shared_ptr<plan::prepared_statement>& out
) {
    yugawara::compiler_result result{};
    switch(r.kind()) {
        case result_kind::execution_plan: {
            result = compile_internal<result_kind::execution_plan>(r, c_options);
            break;
        }
        case result_kind::statement: {
            result = compile_internal<result_kind::statement>(r, c_options);
            break;
        }
        default:
            throw_exception(std::logic_error{""});
    }

    if(!result.success()) {
        auto res = status::err_compiler_error;
        handle_compile_error(result.diagnostics(), res, ctx);
        return res;
    }
    auto stmt = result.release_statement();
    stmt->runtime_hint() = sp->result();
    auto s = std::shared_ptr<::takatori::statement::statement>(std::move(stmt));
    out = std::make_shared<plan::prepared_statement>(
        s,
        result.info(),
        provider,
        preprocess_mirror(s, provider, result.info()),
        ctx.sql_text()
    );
    return status::ok;
}

status parse_validate(
    std::string_view sql,
    compiler_context &ctx,
    std::unique_ptr<shakujo::model::program::Program>& program
) {
    shakujo::parser::Parser parser{};
    try {
        std::stringstream ss{std::string(sql)};
        program = parser.parse_program("<input>", ss);
        shakujo::analyzer::SyntaxValidator validator{};
        shakujo::analyzer::Reporter reporter{};
        if(! validator.analyze(reporter, program.get())) {
            std::stringstream errs{};
            for(auto&& e : reporter.diagnostics()) {
                // skip unresolved place holder
                if(e.code() == shakujo::analyzer::Diagnostic::Code::UNEXPECTED_ELEMENT) {
                    continue;
                }
                errs << e << " ";
            }
            if (errs.str().empty()) {
                return status::ok;
            }
            std::stringstream msg{};
            msg << "syntax validation failed: " << errs.str();
            auto res = status::err_parse_error;
            VLOG_LP(log_error) << res << ": " << msg.str();
            set_compile_error(
                ctx,
                error_code::syntax_exception, // TODO revisit after mizugaki upgrade
                msg.str(),
                res
            );
            return res;
        }
    } catch (shakujo::parser::Parser::Exception &e) {
        std::stringstream msg{};
        msg << "parsing statement failed: " << e.message() << " (" << e.region() << ")";
        auto res = status::err_parse_error;
        VLOG_LP(log_error) << res << ": " <<  msg.str();
        set_compile_error(
            ctx,
            error_code::syntax_exception, // TODO revisit after mizugaki upgrade
            msg.str(),
            res
        );
        return res;
    }
    return status::ok;
}


status prepare(
    std::string_view sql,
    compiler_context &ctx,
    std::shared_ptr<plan::prepared_statement>& out
) {
    ctx.sql_text(std::make_shared<std::string>(sql));

    std::unique_ptr<shakujo::model::program::Program> program{};
    if(auto res = parse_validate(sql, ctx, program); res != status::ok) {
        return res;
    }

    shakujo_translator translator;
    shakujo_translator_options options {
        ctx.storage_provider(),
        ctx.variable_provider(),
        ctx.function_provider(),
        ctx.aggregate_provider(),
        ctx.variable_provider()
    };
    // used when DDL has no parenthesis for decimal: "create table T (c0 decimal)"
    options.default_decimal_precision() = 38;

    yugawara::runtime_feature_set runtime_features {
        //TODO enable features
//        yugawara::runtime_feature::broadcast_exchange,

        yugawara::runtime_feature::aggregate_exchange,

//        yugawara::runtime_feature::broadcast_join_scan,
    };

    auto cfg = global::config_pool();
    if(cfg && cfg->enable_index_join()) {
        runtime_features.insert(yugawara::runtime_feature::index_join);
    }
    std::shared_ptr<yugawara::analyzer::index_estimator> indices {};

    auto sp = std::make_shared<storage_processor>();
    yugawara::compiler_options c_options{
        runtime_features,
        sp,
        indices,
    };

    ::takatori::document::document_map documents;
    auto r = translator(options, *program->main(), documents);
    if (! r) {
        auto res = status::err_compiler_error;
        auto errors = r.release<result_kind::diagnostics>();
        handle_compile_error(errors, res, ctx);
        return res;
    }
    return create_prepared_statement(r, ctx.variable_provider(), c_options, sp, ctx, out);
}

executor::process::step create(
    takatori::plan::process const& process,
    compiled_info const& info,
    std::shared_ptr<mirror_container> const& mirrors,
    variable_table const* host_variables
) {
    auto& mirror = mirrors->at(std::addressof(process));
    auto pinfo = std::make_shared<executor::process::processor_info>(
        const_cast<relation::graph_type&>(process.operators()),
        info,
        mirror.first,
        mirror.second,
        host_variables
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
    return {
        std::move(pinfo),
        std::make_shared<executor::process::relation_io_map>(std::move(inputs), std::move(outputs))
    };
}

executor::exchange::forward::step create(
    takatori::plan::forward const& forward,
    compiled_info const& info
) {
    meta::variable_order column_order{
        meta::variable_ordering_enum_tag<meta::variable_ordering_kind::flat_record>,
        forward.columns(),
    };
    std::vector<meta::field_type> fields{};
    auto cnt = forward.columns().size();
    fields.reserve(cnt);
    for(auto&& c: forward.columns()) {
        fields.emplace_back(utils::type_for(info, c));
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
    compiled_info const& info
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
        fields.emplace_back(utils::type_for(info, c));
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
            std::move(sort_ordering),
            group.limit()
        ),
        std::move(input_order),
        std::move(output_order)
    );
}

executor::exchange::aggregate::step create(
    takatori::plan::aggregate const& agg,
    compiled_info const& info
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
        fields.emplace_back(utils::type_for(info, c));
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
            utils::type_for(info, e.destination())
        );
    }
    return {
        std::make_shared<aggregate_info>(
            std::move(meta),
            std::move(key_indices),
            std::move(specs),
            agg.mode() == takatori::plan::group_mode::equivalence_or_whole && agg.group_keys().empty()
        ),
        std::move(input_order),
        std::move(output_order)
    };
}

std::shared_ptr<executor::process::impl::variable_table_info> create_host_variable_info(
    std::shared_ptr<::yugawara::variable::configurable_provider> const& provider,
    compiled_info const& info
) {
    if (provider == nullptr) {
        return {};
    }
    using ::yugawara::variable::declaration;
    std::size_t count = 0;
    std::unordered_map<takatori::descriptor::variable, std::size_t> map{};
    std::vector<meta::field_type> fields{};
    yugawara::binding::factory bindings{};
    std::unordered_map<std::string, takatori::descriptor::variable> names{};
    provider->each([&](std::shared_ptr<declaration const> const& decl){
        auto v = bindings(decl);
        if(auto r = info.variables().find(v)) {
            if (r.kind() == yugawara::analyzer::variable_resolution_kind::external) {
                fields.emplace_back(utils::type_for(decl->type()));
                map[v] = count;
                names.emplace(decl->name(), v);
                ++count;
            }
        }
    });
    boost::dynamic_bitset<std::uint64_t> nullability{};
    nullability.resize(count, true); // currently stream variables are all nullable
    return std::make_shared<executor::process::impl::variable_table_info>(
        std::move(map),
        names,
        std::make_shared<meta::record_meta>(std::move(fields), std::move(nullability))
    );
}

status validate_host_variables(
    compiler_context& ctx,
    parameter_set const* parameters,
    std::shared_ptr<executor::process::impl::variable_table_info> const& info
) {
    if(! info) return status::ok;
    for(auto it = info->name_list_begin(); it != info->name_list_end(); ++it) {
        auto& name = it->first;
        if(! parameters->find(name)) {
            std::stringstream ss{};
            ss << "Value is not assigned for host variable '" << name << "'";
            auto res = status::err_unresolved_host_variable;
            VLOG_LP(log_error) << res << ": " << ss.str();
            set_compile_error(
                ctx,
                error_code::unresolved_placeholder_exception,
                ss.str(),
                res
            );
            return res;
        }
    }
    return status::ok;
}

std::shared_ptr<executor::process::impl::variable_table> create_host_variables(
    parameter_set const* parameters,
    std::shared_ptr<executor::process::impl::variable_table_info> const& info
) {
    if (parameters == nullptr || info == nullptr) {
        return {};
    }
    using ::yugawara::variable::declaration;
    auto vars = std::make_shared<executor::process::impl::variable_table>(*info);
    auto target = vars->store().ref();
    for(auto& [name, e] : *parameters) {
        if(! info->exists(name)) {
            VLOG_LP(log_warning) << "Parameter '" << name << "' is passed but not used by the statement";
            continue;
        }
        auto os = info->at(name);
        utils::copy_nullable_field(
            e.type(),
            target,
            os.value_offset(),
            os.nullity_offset(),
            e.as_any()
        );
    }
    return vars;
}

void create_mirror_for_write(
    compiler_context& ctx,
    maybe_shared_ptr<statement::statement> statement,
    compiled_info info,
    std::shared_ptr<mirror_container> const& mirrors,
    parameter_set const* parameters
) {
    auto vars = create_host_variables(parameters, mirrors->host_variable_info());
    auto& node = unsafe_downcast<statement::write>(*statement);
    auto& index = yugawara::binding::extract<yugawara::storage::index>(node.destination());
    auto write = std::make_shared<executor::common::write>(
        executor::process::impl::ops::write_kind_from(node.operator_kind()),
        index,
        node,
        *ctx.resource(),
        info,
        vars.get()
    );
    BOOST_ASSERT( //NOLINT
        node.operator_kind() == relation::write_kind::insert ||
            node.operator_kind() == relation::write_kind::insert_overwrite ||
            node.operator_kind() == relation::write_kind::insert_skip
    );
    ctx.executable_statement(
        std::make_shared<executable_statement>(
            std::move(statement),
            std::move(info),
            std::move(write),
            mirrors->host_variable_info(),
            std::move(vars),
            mirrors,
            ctx.sql_text_shared()
        )
    );
}

void create_mirror_for_ddl(
    compiler_context& ctx,
    maybe_shared_ptr<statement::statement> statement,
    compiled_info info,
    std::shared_ptr<mirror_container> const& mirrors,
    parameter_set const* parameters
) {
    maybe_shared_ptr<model::statement> ops{};
    switch(statement->kind()) {
        case statement::statement_kind::create_table: {
            auto& node = unsafe_downcast<statement::create_table>(*statement);
            ops = std::make_shared<executor::common::create_table>(node);
            break;
        }
        case statement::statement_kind::drop_table: {
            auto& node = unsafe_downcast<statement::drop_table>(*statement);
            ops = std::make_shared<executor::common::drop_table>(node);
            break;
        }
        case statement::statement_kind::create_index: {
            auto& node = unsafe_downcast<statement::create_index>(*statement);
            ops = std::make_shared<executor::common::create_index>(node);
            break;
        }
        case statement::statement_kind::drop_index: {
            auto& node = unsafe_downcast<statement::drop_index>(*statement);
            ops = std::make_shared<executor::common::drop_index>(node);
            break;
        }
        default:
            throw_exception(std::logic_error{""});
    }
    auto vars = create_host_variables(parameters, mirrors->host_variable_info());
    ctx.executable_statement(
        std::make_shared<executable_statement>(
            std::move(statement),
            std::move(info),
            std::move(ops),
            mirrors->host_variable_info(),
            std::move(vars),
            mirrors,
            ctx.sql_text_shared()
        )
    );
}

void create_mirror_for_execute(
    compiler_context& ctx,
    maybe_shared_ptr<statement::statement> statement,
    compiled_info info,
    std::shared_ptr<mirror_container> const& mirrors,
    parameter_set const* parameters
) {
    auto vars = create_host_variables(parameters, mirrors->host_variable_info());
    std::unordered_map<takatori::plan::step const*, executor::common::step*> steps{};
    yugawara::binding::factory bindings{};
    auto mirror = std::make_shared<executor::common::graph>();
    takatori::plan::sort_from_upstream(
        unsafe_downcast<takatori::statement::execute>(*statement).execution_plan(),
        [&mirror, &info, &bindings, &steps, &vars, &mirrors](takatori::plan::step const& s){
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
                    throw_exception(std::logic_error{""});
                    break;
                case takatori::plan::step_kind::discard:
                    throw_exception(std::logic_error{""});
                    break;
                case takatori::plan::step_kind::process: {
                    auto& process = unsafe_downcast<takatori::plan::process const>(s);  //NOLINT
                    steps[&process] = &mirror->emplace<executor::process::step>(create(process, info, mirrors, vars.get()));
                    break;
                }
                default:
                    break;
            }
        }
    );

    using model::step_kind;
    for(auto&& [s, step] : steps) {
        auto map = std::make_shared<executor::process::io_exchange_map>();
        if(takatori::plan::has_upstream(*s)) {
            takatori::plan::enumerate_upstream(
                *s,
                [step=step, &steps, &map](takatori::plan::step const& up){
                    // assuming enumerate_upstream respects the input port ordering TODO confirm
                    *step << *steps[&up];
                    if(step->kind() == step_kind::process) {
                        map->add_input(unsafe_downcast<executor::exchange::step>(steps[&up]));
                    }
                }
            );
        }
        if(takatori::plan::has_downstream(*s)) {
            takatori::plan::enumerate_downstream(
                *s,
                [step=step, &steps, &map](takatori::plan::step const& down){
                    if(step->kind() == step_kind::process) {
                        map->add_output(unsafe_downcast<executor::exchange::step>(steps[&down]));
                    }
                }
            );
        }
        if(step->kind() == step_kind::process) {
            unsafe_downcast<executor::process::step>(step)->io_exchange_map(std::move(map));
        }
    }
    ctx.executable_statement(std::make_shared<executable_statement>(
        std::move(statement),
        std::move(info),
        std::make_shared<executor::common::execute>(mirror),
        mirrors->host_variable_info(),
        std::move(vars),
        mirrors,
        ctx.sql_text_shared()
    ));
}

/**
 * @brief compile prepared statement, resolve parameters, and generate executable statement
 * @pre storage provider exists and populated in the compiler context
 */
status create_executable_statement(compiler_context& ctx, parameter_set const* parameters) {
    using takatori::statement::statement_kind;
    auto p = ctx.prepared_statement();
    BOOST_ASSERT(p != nullptr); //NOLINT
    if(auto res = validate_host_variables(ctx, parameters, p->mirrors()->host_variable_info()); res != status::ok) {
        return res;
    }
    ctx.sql_text(p->sql_text_shared()); // compiler context doesn't always have sql text, so copy from prepared statement
    switch(p->statement()->kind()) {
        case statement_kind::write:
            create_mirror_for_write(ctx, p->statement(), p->compiled_info(), p->mirrors(), parameters);
            break;
        case statement_kind::execute:
            create_mirror_for_execute(ctx, p->statement(), p->compiled_info(), p->mirrors(), parameters);
            break;
        case statement_kind::create_table:
            create_mirror_for_ddl(ctx, p->statement(), p->compiled_info(), p->mirrors(), parameters);
            break;
        case statement_kind::drop_table:
            create_mirror_for_ddl(ctx, p->statement(), p->compiled_info(), p->mirrors(), parameters);
            break;
        case statement_kind::create_index:
            create_mirror_for_ddl(ctx, p->statement(), p->compiled_info(), p->mirrors(), parameters);
            break;
        case statement_kind::drop_index:
            create_mirror_for_ddl(ctx, p->statement(), p->compiled_info(), p->mirrors(), parameters);
            break;
        default:
            throw_exception(std::logic_error{""});
    }
    return status::ok;
}

} // namespace impl

status prepare(std::string_view sql, compiler_context &ctx) {
    try {
        std::shared_ptr<prepared_statement> stmt{};
        auto rc = impl::prepare(sql, ctx, stmt);
        if (rc == status::ok) {
            ctx.prepared_statement(std::move(stmt));
        }
        return rc;
    } catch (impl::exception const& e) {
        ctx.error_info(e.info());
        return e.info()->status();
    }
}

status compile(
    compiler_context &ctx,
    parameter_set const* parameters
) {
    try {
        return impl::create_executable_statement(ctx, parameters);
    } catch (impl::exception const& e) {
        ctx.error_info(e.info());
        return e.info()->status();
    }
}

status compile(
    std::string_view sql,
    compiler_context &ctx,
    parameter_set const* parameters
) {
    try {
        if(auto rc = prepare(sql, ctx); rc != status::ok) {
            return rc;
        }
        return impl::create_executable_statement(ctx, parameters);
    } catch (impl::exception const& e) {
        ctx.error_info(e.info());
        return e.info()->status();
    }
}

}