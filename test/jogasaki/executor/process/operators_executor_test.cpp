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
#include <jogasaki/executor/process/impl/relop/operators_executor.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/executor/comparator.h>

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

#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::executor::process::impl::relop {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
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

namespace tinfo = ::shakujo::common::core::type;

class operators_executor_test : public test_root {

public:
    using kind = field_type_kind;

    std::unique_ptr<shakujo::model::program::Program> gen_shakujo_program(std::string_view sql) {
        shakujo::parser::Parser parser;
        std::unique_ptr<shakujo::model::program::Program> program;
        try {
            std::stringstream ss{std::string(sql)};
            program = parser.parse_program("compiler_test", ss);
        } catch (shakujo::parser::Parser::Exception &e) {
            LOG(ERROR) << "parse error:" << e.message() << " (" << e.region() << ")" << std::endl;
        }
        return program;
    };

    std::shared_ptr<::yugawara::storage::configurable_provider> yugawara_provider() {

        std::shared_ptr<::yugawara::storage::configurable_provider> storages
                = std::make_shared<::yugawara::storage::configurable_provider>();

        std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table("T0", {
                "T1",
                {
                        { "C0", type::int4() },
                        { "C1", type::float8 () },
                },
        });
        std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index("I0", {
                t0,
                "I0",
                {
                        t0->columns()[0],
                },
                {},
                {
                        ::yugawara::storage::index_feature::find,
                        ::yugawara::storage::index_feature::scan,
                        ::yugawara::storage::index_feature::unique,
                        ::yugawara::storage::index_feature::primary,
                },
        });
        return storages;
    }

};

TEST_F(operators_executor_test, scan_emit) {
    std::string sql = "select * from T0";
    auto p = gen_shakujo_program(sql);
    auto storages = yugawara_provider();

    shakujo_translator translator;
    shakujo_translator_options options {
            storages,
            {},
            {},
            {},
    };

    placeholder_map placeholders;
    ::takatori::document::document_map documents;
    ::shakujo::model::IRFactory ir;
    ::yugawara::binding::factory bindings { options.get_object_creator() };

    auto r = translator(options, *p->main(), documents, placeholders);
    ASSERT_EQ(r.kind(), result_kind::execution_plan);

    auto ptr = r.release<result_kind::execution_plan>();
    auto&& graph = *ptr;
    auto&& emit = testing::last<relation::emit>(graph);
    auto&& scan = testing::next<relation::scan>(emit.input());

    ASSERT_EQ(scan.columns().size(), 2);
    ASSERT_EQ(emit.columns().size(), 2);

    EXPECT_EQ(emit.columns()[0].source(), scan.columns()[0].destination());
    EXPECT_EQ(emit.columns()[1].source(), scan.columns()[1].destination());
    EXPECT_EQ(emit.columns()[0].name(), "C0");
    EXPECT_EQ(emit.columns()[1].name(), "C1");

//    EXPECT_EQ(bindings(emit.columns()[0].source()), "C0");

//    auto c0 = bindings.stream_variable("c0");
//    auto c1 = bindings.stream_variable("c1");
//    auto&& in = r.insert(relation::scan {
//            bindings(*i0),
//            {
//                    { bindings(t0c0), c0 },
//                    { bindings(t0c1), c1 },
//            },
//    });
//    auto&& out = r.insert(relation::emit { c0 });
//    in.output() >> out.input();

    yugawara::runtime_feature_set runtime_features { yugawara::compiler_options::default_runtime_features };
    std::shared_ptr<yugawara::analyzer::index_estimator> indices {};

    auto t0 = storages->find_relation("T0");
    yugawara::storage::column const& t0c0 = t0->columns()[0];
    yugawara::storage::column const& t0c1 = t0->columns()[1];

//    std::shared_ptr<yugawara::storage::index> i0 = storages->add_index("I0", { t0, "I0", });

    yugawara::compiler_options c_options{
            indices,
            runtime_features,
            options.get_object_creator(),
    };
    auto result = yugawara::compiler()(c_options, std::move(graph));
    ASSERT_TRUE(result);

    auto&& c = downcast<statement::execute>(result.statement());

    ASSERT_EQ(c.execution_plan().size(), 1);
    auto&& p0 = testing::find(c.execution_plan(), scan);
    auto&& p1 = testing::find(c.execution_plan(), emit);
    ASSERT_EQ(p0, p1);

    ASSERT_EQ(p0.operators().size(), 2);
    ASSERT_TRUE(p0.operators().contains(scan));
    ASSERT_TRUE(p0.operators().contains(emit));

    ASSERT_EQ(scan.columns().size(), 2);
    EXPECT_EQ(scan.columns()[0].source(), bindings(t0c0));
    EXPECT_EQ(scan.columns()[1].source(), bindings(t0c1));
    auto&& c0p0 = scan.columns()[0].destination();
    auto&& c1p0 = scan.columns()[1].destination();

    ASSERT_EQ(emit.columns().size(), 2);
    EXPECT_EQ(emit.columns()[0].source(), c0p0);
    EXPECT_EQ(emit.columns()[1].source(), c1p0);

    EXPECT_EQ(result.type_of(bindings(t0c0)), type::int4());
    EXPECT_EQ(result.type_of(c0p0), type::int4());
    EXPECT_EQ(result.type_of(bindings(t0c1)), type::float8());
    EXPECT_EQ(result.type_of(c1p0), type::float8());

    auto& ops = p0.operators();

    auto meta = test_record_meta1();

    memory::page_pool pool;
    memory::monotonic_paged_memory_resource record_resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    auto store = std::make_shared<data::record_store>(&record_resource, &varlen_resource, test_record_meta1());

    operators_executor e{ops, {}, {}}; //TODO
    e.process();
}

}

