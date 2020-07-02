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
#include <functional>

#include <glog/logging.h>

#include <shakujo/parser/Parser.h>
#include <shakujo/common/core/Type.h>
#include <shakujo/model/IRFactory.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/binding/factory.h>
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
#include <takatori/serializer/json_printer.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/plan/graph.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/process/step.h>
#include "compiler_context.h"

namespace jogasaki::plan {

///@private
namespace impl {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

using namespace ::mizugaki::translator;
using namespace ::mizugaki;

using code = shakujo_translator_code;
using result_kind = shakujo_translator::result_type::kind_type;

namespace statement = ::takatori::statement;

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

void create_step_graph(std::string_view sql, compiler_context& ctx) {
    auto p = generate_program(sql);

    shakujo_translator translator;
    shakujo_translator_options options {
        ctx.storage_provider(),
        {},
        {},
        {},
    };

    placeholder_map placeholders;
    ::takatori::document::document_map documents;

    auto r = translator(options, *p->main(), documents, placeholders);
    auto ptr = r.release<result_kind::execution_plan>();
    auto&& graph = *ptr;

    yugawara::runtime_feature_set runtime_features { yugawara::compiler_options::default_runtime_features };
    std::shared_ptr<yugawara::analyzer::index_estimator> indices {};

    yugawara::compiler_options c_options{
        indices,
        runtime_features,
        options.get_object_creator(),
    };
    auto result = yugawara::compiler()(c_options, std::move(graph));
    if(!result.success()) {
        fail();
    }
    ctx.compiler_result(std::move(result));
}

void create_mirror(compiler_context& ctx) {
    auto& statement = ctx.compiler_result().statement();
    using statement_kind = takatori::statement::statement_kind;
    switch(statement.kind()) {
        case statement_kind::execute: {
            auto&& c = downcast<statement::execute>(statement);
            auto& g = c.execution_plan();
            auto mirror = std::make_shared<executor::common::graph>();
            takatori::plan::enumerate_top(g, [&mirror](takatori::plan::step const& s){
                // TODO implement others than process
                (void)s;
                mirror->emplace<executor::process::step>();
            });
            ctx.step_graph(std::move(mirror));
            break;
        }
        case statement_kind::write:
        case statement_kind::extension:
            fail();
    }
}

} // namespace impl

void compile(std::string_view sql, compiler_context& ctx) {
    impl::create_step_graph(sql, ctx);
    impl::create_mirror(ctx);
}


}
