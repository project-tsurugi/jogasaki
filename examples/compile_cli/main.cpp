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
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <takatori/document/document_map.h>
#include <takatori/graph/graph.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/serializer/object_scanner.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/value/primitive.h>
#include <takatori/value/value_kind.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/analyzer/index_estimator.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>
#include <yugawara/compiler_result.h>
#include <yugawara/diagnostic.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/serializer/object_scanner.h>
#include <yugawara/storage/configurable_provider.h>
#include <mizugaki/placeholder_entry.h>
#include <mizugaki/placeholder_map.h>
#include <mizugaki/translator/shakujo_translator.h>
#include <mizugaki/translator/shakujo_translator_code.h>
#include <mizugaki/translator/shakujo_translator_options.h>
#include <mizugaki/translator/shakujo_translator_result.h>
#include <shakujo/common/core/DocumentRegion.h>
#include <shakujo/model/program/Program.h>
#include <shakujo/parser/Parser.h>

#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/tables.h>

namespace jogasaki::compile_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace takatori::util;

using namespace ::mizugaki::translator;
using namespace ::mizugaki;

using code = shakujo_translator_code;
using result_kind = shakujo_translator::result_type::kind_type;

namespace type = ::takatori::type;
namespace value = ::takatori::value;

std::unique_ptr<shakujo::model::program::Program> shakujo_program(std::string_view sql) {
    shakujo::parser::Parser parser;
    std::unique_ptr<shakujo::model::program::Program> program;
    try {
        std::stringstream ss{std::string(sql)};
        program = parser.parse_program("compile_cli", ss);
    } catch (shakujo::parser::Parser::Exception &e) {
        LOG(ERROR) << "shakujo parse error:" << e.message() << " (" << e.region() << ")" << std::endl;
    }
    return program;
};

std::shared_ptr<::yugawara::storage::configurable_provider> tables() {
    std::shared_ptr<::yugawara::storage::configurable_provider> storages
        = std::make_shared<::yugawara::storage::configurable_provider>();
    executor::add_builtin_tables(*storages);
    return storages;
}

std::shared_ptr<::yugawara::aggregate::configurable_provider> aggregate_functions() {
    auto ret = std::make_shared<::yugawara::aggregate::configurable_provider>();
    executor::function::incremental::add_builtin_aggregate_functions(*ret, global::incremental_aggregate_function_repository());
    return ret;
}

[[maybe_unused]] static void dump(yugawara::compiler_result const& r) {
    r.object_scanner()(
        r.statement(),
        ::takatori::serializer::json_printer { std::cout });
}

static int run(std::string_view sql) {
    if (sql.empty()) return 0;
    auto p = shakujo_program(sql);
    auto storages = tables();
    executor::add_test_tables(*storages);
    executor::add_benchmark_tables(*storages);
    auto agg_functions = aggregate_functions();

    shakujo_translator translator;
    shakujo_translator_options options {
        storages,
        {},
        {},
        agg_functions,
    };

    yugawara::runtime_feature_set runtime_features {
//        yugawara::runtime_feature::broadcast_exchange,
        yugawara::runtime_feature::aggregate_exchange,
        yugawara::runtime_feature::index_join,
//        yugawara::runtime_feature::broadcast_join_scan,
    };
    std::shared_ptr<yugawara::analyzer::index_estimator> indices {};

    yugawara::compiler_options c_options{
        runtime_features,
        {},
        indices,
    };

    placeholder_map placeholders{};
    ::takatori::document::document_map documents{};
    placeholders.add("p0", {type::int8(), value::int8(1)});
    placeholders.add("p1", {type::float8(), value::float8(10.0)});
    auto r = translator(options, *p->main(), documents, placeholders);
    if (!r.is_valid()) {
        auto error = r.release<result_kind::diagnostics>();
        for(auto&& e : error) {
            std::cerr << e.message() << "; code " << e.code() << std::endl;
        }
        return -1;
    }
    yugawara::compiler_result result{};
    switch(r.kind()) {
        case result_kind::execution_plan: {
            auto ptr = r.release<result_kind::execution_plan>();
            result = yugawara::compiler()(c_options, std::move(*ptr));
            break;
        }
        case result_kind::statement: {
            auto ptr = r.release<result_kind::statement>();
            result = yugawara::compiler()(c_options, std::move(*ptr));
            break;
        }
        default:
            std::abort();
    }
//    dump(result);

    // TODO display jogasaki graph info too.

    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("compile cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("compile cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }

    std::string_view source { argv[1] }; // NOLINT
    try {
        jogasaki::compile_cli::run(source);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
