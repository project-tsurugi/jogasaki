/*
 * Copyright 2018-2019 tsurugi project.
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
#include <iostream>
#include <vector>

#include <glog/logging.h>

#include <shakujo/parser/Parser.h>
#include <shakujo/common/core/Type.h>

#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/storage/configurable_provider.h>
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

#include <jogasaki/executor/global.h>
#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/executor/function/builtin_functions.h>
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
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

using ::yugawara::variable::criteria;
using ::yugawara::variable::nullity;
namespace tinfo = ::shakujo::common::core::type;

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

    static constexpr auto not_nullable = nullity{false};
    std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table("T0", {
        "T0",
        {
            { "C0", type::int8(), not_nullable },
            { "C1", type::float8(), not_nullable },
        },
    });
    std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index("I0", {
        t0,
        "I0",
        {
            t0->columns()[0],
        },
        {
            t0->columns()[1],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });
    std::shared_ptr<::yugawara::storage::table> t1 = storages->add_table("T1", {
        "T1",
        {
            { "C0", type::int8(), not_nullable },
            { "C1", type::float8(), not_nullable },
        },
    });
    std::shared_ptr<::yugawara::storage::index> i1 = storages->add_index("I1", {
        t1,
        "I1",
        {
            t1->columns()[0],
        },
        {
            t1->columns()[1],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });
    std::shared_ptr<::yugawara::storage::table> t2 = storages->add_table("T2", {
        "T1",
        {
            { "C0", type::int8(), not_nullable },
            { "C1", type::float8(), not_nullable },
        },
    });
    std::shared_ptr<::yugawara::storage::index> i2 = storages->add_index("I2", {
        t2,
        "I2",
        {
            t2->columns()[0],
        },
        {
            t2->columns()[1],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });
    std::shared_ptr<::yugawara::storage::table> t3 = storages->add_table("T3", {
        "T3",
        {
            { "C0", type::int8(), not_nullable },
            { "C1", type::int8(), not_nullable },
            { "C2", type::int8(), not_nullable },
        },
    });
    std::shared_ptr<::yugawara::storage::index> i3 = storages->add_index("I3", {
        t3,
        "I3",
        {
            t3->columns()[0],
        },
        {
            t3->columns()[1],
            t3->columns()[2],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });
    return storages;
}

std::shared_ptr<::yugawara::aggregate::configurable_provider> aggregate_functions() {
    auto ret = std::make_shared<::yugawara::aggregate::configurable_provider>();
    executor::function::add_builtin_aggregate_functions(*ret, global::function_repository());
    return ret;
}

static void dump(yugawara::compiler_result const& r) {
    ::takatori::serializer::json_printer printer { std::cout };
    r.object_scanner()(
        r.statement(),
        ::takatori::serializer::json_printer { std::cout });
}

static int run(std::string_view sql) {
    if (sql.empty()) return 0;
    auto p = shakujo_program(sql);
    auto storages = tables();
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
    auto r = translator(options, *p->main(), documents, placeholders);
    if (!r.is_valid()) {
        auto error = r.release<result_kind::diagnostics>();
        for(auto e : error) {
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
    dump(result);

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
