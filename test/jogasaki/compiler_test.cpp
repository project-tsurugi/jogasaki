/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/graph/graph.h>
#include <takatori/graph/graph_iterator.h>
#include <takatori/plan/exchange.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/process.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/project.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/relation/step/flatten.h>
#include <takatori/relation/step/join.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_cogroup.h>
#include <takatori/relation/step/take_group.h>
#include <takatori/relation/write_kind.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/statement/execute.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/statement/write.h>
#include <takatori/tree/tree_element_vector.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/exception.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/value/primitive.h>
#include <takatori/value/value_kind.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/binding/extract.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/relation.h>
#include <yugawara/storage/table.h>
#include <yugawara/util/object_cache.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/utils/field_types.h>

#include "test_utils.h"

namespace jogasaki::plan {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

using namespace testing;

using namespace ::yugawara::variable;
using namespace ::yugawara;

/**
 * @brief test to confirm the compiler behavior
 * TOOO this is temporary, do not depend on compiler to generate same plan
 */
class compiler_test : public ::testing::Test {
public:
    using kind = field_type_kind;
    ::yugawara::binding::factory bindings{};

    std::shared_ptr<::yugawara::storage::configurable_provider> tables() {

        std::shared_ptr<::yugawara::storage::configurable_provider> storages
            = std::make_shared<::yugawara::storage::configurable_provider>();

        std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table({
            "T0",
            {
                { "C0", type::int8(), criteria{nullity{false}}},
                { "C1", type::float8 () },
            },
        });
        std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index({
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
        std::shared_ptr<::yugawara::storage::table> t1 = storages->add_table({
            "T1",
            {
                { "C0", type::int8(), criteria{nullity{false}}},
                { "C1", type::float8 () },
            },
        });
        std::shared_ptr<::yugawara::storage::index> i1 = storages->add_index({
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
        return storages;
    }

    std::shared_ptr<::yugawara::aggregate::configurable_provider> aggregate_functions() {
        auto provider = std::make_shared<::yugawara::aggregate::configurable_provider>();
        executor::function::incremental::add_builtin_aggregate_functions(*provider, global::incremental_aggregate_function_repository());
        return provider;
    }
};

TEST_F(compiler_test, insert) {
    std::string sql = "insert into T0(C0, C1) values (1,1.0E0)";
    memory::page_pool pool{};
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&pool);
    compiler_context ctx{};
    ctx.resource(resource);
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto&& write = unsafe_downcast<statement::write>(*ctx.executable_statement()->statement());

    EXPECT_EQ(write.operator_kind(), relation::write_kind::insert);

    ASSERT_EQ(write.columns().size(), 2);
    auto t0 = ctx.storage_provider()->find_relation("T0");
    EXPECT_EQ(write.columns()[0], bindings(t0->columns()[0]));
    EXPECT_EQ(write.columns()[1], bindings(t0->columns()[1]));

    ASSERT_EQ(write.tuples().size(), 1);
    auto&& es = write.tuples()[0].elements();
    ASSERT_EQ(es.size(), 2);
    EXPECT_EQ(es[0], scalar::immediate(value::int8(1), type::int8()));
    EXPECT_EQ(es[1], scalar::immediate(value::float8(1.0), type::float8()));

    auto& info =ctx.executable_statement()->compiled_info();
    auto& stmt =*ctx.executable_statement()->statement();
//    dump(info, stmt);
}

TEST_F(compiler_test, simple_query) {
    std::string sql = "select * from T0";

    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c = downcast<statement::execute>(stmt);

    ASSERT_EQ(c.execution_plan().size(), 1);
    auto&& p0 = top(c.execution_plan());

    ASSERT_EQ(p0.operators().size(), 2);

    auto&& scan = head<takatori::relation::scan>(p0.operators());

    auto&& emit = next_relation<takatori::relation::emit>(scan);
    ASSERT_TRUE(p0.operators().contains(scan));
    ASSERT_TRUE(p0.operators().contains(emit));

    ASSERT_EQ(scan.columns().size(), 2);
//    EXPECT_EQ(scan.columns()[0].source(), bindings(t0c0));
//    EXPECT_EQ(scan.columns()[1].source(), bindings(t0c1));
    auto&& c0p0 = scan.columns()[0].destination();
    auto&& c1p0 = scan.columns()[1].destination();

    // verify extract
    auto& t0c0 = yugawara::binding::extract<yugawara::storage::column>(scan.columns()[0].source());
    auto& t0c1 = yugawara::binding::extract<yugawara::storage::column>(scan.columns()[1].source());
    EXPECT_EQ("C0", t0c0.simple_name());
    EXPECT_EQ("C1", t0c1.simple_name());
    EXPECT_FALSE(t0c0.criteria().nullity().nullable());
    EXPECT_TRUE(t0c1.criteria().nullity().nullable());

    ASSERT_EQ(emit.columns().size(), 2);
    EXPECT_EQ(emit.columns()[0].source(), c0p0);
    EXPECT_EQ(emit.columns()[1].source(), c1p0);

    EXPECT_EQ(info.type_of(c0p0), type::int8());
    EXPECT_EQ(info.type_of(c1p0), type::float8());

//    dump(info, stmt);

    // test utils
    EXPECT_EQ(meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>), utils::type_for(info, c0p0));
    EXPECT_EQ(meta::field_type(meta::field_enum_tag<meta::field_type_kind::float8>), utils::type_for(info, c1p0));

}

TEST_F(compiler_test, filter) {
    std::string sql = "select C0 from T0 where C1=1.0";
    compiler_context ctx{};
    ctx.storage_provider(tables());
   ASSERT_EQ(status::ok, compile(sql, ctx));

    auto& info =ctx.executable_statement()->compiled_info();
    auto& stmt =*ctx.executable_statement()->statement();
    dump(info, stmt);
    auto&& c = downcast<statement::execute>(stmt);
    ASSERT_EQ(c.execution_plan().size(), 1);

    auto b = c.execution_plan().begin();
    auto&& graph = takatori::util::downcast<takatori::plan::process>(*b).operators();
    auto&& emit = last<relation::emit>(graph);
    auto&& project = next<relation::project>(emit.input());
    auto&& filter = next<relation::filter>(project.input());
    auto&& scan = next<relation::scan>(filter.input());

    auto&& p0 = find(c.execution_plan(), scan);
    auto&& p1 = find(c.execution_plan(), emit);
    auto&& p2 = find(c.execution_plan(), filter);
    auto&& p3 = find(c.execution_plan(), project);
    ASSERT_EQ(p0, p1);
    ASSERT_EQ(p1, p2);
    ASSERT_EQ(p2, p3);

    ASSERT_EQ(p0.operators().size(), 4);
    ASSERT_TRUE(p0.operators().contains(scan));
    ASSERT_TRUE(p0.operators().contains(filter));
    ASSERT_TRUE(p0.operators().contains(emit));
    ASSERT_TRUE(p0.operators().contains(project));

    ASSERT_EQ(scan.columns().size(), 2);
//    EXPECT_EQ(scan2.columns()[0].source(), bindings(t0c0));
//    EXPECT_EQ(scan2.columns()[1].source(), bindings(t0c1));
    auto&& c0p0 = scan.columns()[0].destination();
    auto&& c1p0 = scan.columns()[1].destination();

    ASSERT_EQ(emit.columns().size(), 1);
    // EXPECT_EQ(emit.columns()[0].source(), c0p0);

    EXPECT_EQ(info.type_of(c0p0), type::int8());
}

TEST_F(compiler_test, project_filter) {
    std::string sql = "select C1+C0, C0, C1 from T0 where C1=1.0";
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info =ctx.executable_statement()->compiled_info();
    auto& stmt =*ctx.executable_statement()->statement();
    auto&& c = downcast<statement::execute>(stmt);
//    dump(info, stmt);

    ASSERT_EQ(c.execution_plan().size(), 1);

    auto b = c.execution_plan().begin();
    auto&& graph = takatori::util::downcast<takatori::plan::process>(*b).operators();
    auto&& emit = last<relation::emit>(graph);
    auto&& project = next<relation::project>(emit.input());
    auto&& filter = next<relation::filter>(project.input());
    auto&& scan = next<relation::scan>(filter.input());

    auto&& p0 = find(c.execution_plan(), scan);
    auto&& p1 = find(c.execution_plan(), emit);
    auto&& p2 = find(c.execution_plan(), filter);
    auto&& p3 = find(c.execution_plan(), project);
    ASSERT_EQ(p0, p1);
    ASSERT_EQ(p1, p2);
    ASSERT_EQ(p2, p3);

    ASSERT_EQ(p0.operators().size(), 4);
    ASSERT_TRUE(p0.operators().contains(scan));
    ASSERT_TRUE(p0.operators().contains(filter));
    ASSERT_TRUE(p0.operators().contains(emit));
    ASSERT_TRUE(p0.operators().contains(project));

    ASSERT_EQ(scan.columns().size(), 2);
//    EXPECT_EQ(scan.columns()[0].source(), bindings(t0c0));
//    EXPECT_EQ(scan.columns()[1].source(), bindings(t0c1));
    auto&& c0p0 = scan.columns()[0].destination();
    auto&& c1p0 = scan.columns()[1].destination();

    ASSERT_EQ(emit.columns().size(), 3);
//    EXPECT_EQ(emit.columns()[0].source(), c0p0);

}

TEST_F(compiler_test, join) {
    std::string sql = "select T0.C0, T1.C1 from T0, T0 T1";
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info =ctx.executable_statement()->compiled_info();
    auto& stmt =*ctx.executable_statement()->statement();
    auto&& c = downcast<statement::execute>(stmt);
//    dump(info, stmt);

    ASSERT_EQ(c.execution_plan().size(), 5);

    auto& b = top(c.execution_plan());
    auto&& graph = takatori::util::downcast<takatori::plan::process>(b).operators();
    auto&& offer = last<relation::step::offer>(graph);
    auto&& scan = next<relation::scan>(offer.input());
    {
        auto&& p0 = find(c.execution_plan(), scan);
        auto&& p1 = find(c.execution_plan(), offer);
        ASSERT_EQ(p0, p1);
    }

    auto& b2 = next_top(c.execution_plan(), b);
    auto&& graph2 = takatori::util::downcast<takatori::plan::process>(b2).operators();
    auto&& offer2 = last<relation::step::offer>(graph2);
    auto&& scan2 = next<relation::scan>(offer2.input());
    {
        auto&& p0 = find(c.execution_plan(), scan2);
        auto&& p1 = find(c.execution_plan(), offer2);
        ASSERT_EQ(p0, p1);
    }

    auto& grp1 = b.downstreams()[0];
    auto& grp2 = b2.downstreams()[0];

    auto mirrors = std::make_shared<mirror_container>();
    jogasaki::plan::impl::preprocess(b, info, mirrors);
    auto s = jogasaki::plan::impl::create(b, info, mirrors, nullptr);
    auto io_map = s.relation_io_map();
    ASSERT_EQ(0, io_map->output_index(bindings(grp1)));
}

TEST_F(compiler_test, left_outer_join) {
    std::string sql = "select T0.C0, T1.C1 from T0 LEFT OUTER JOIN T1 ON T0.C1 = T1.C1";
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info =ctx.executable_statement()->compiled_info();
    auto& stmt =*ctx.executable_statement()->statement();
    auto&& c = downcast<statement::execute>(stmt);
    dump(info, stmt);

    ASSERT_EQ(c.execution_plan().size(), 5);

    auto& b = top(c.execution_plan());
    auto&& graph = takatori::util::downcast<takatori::plan::process>(b).operators();
    auto&& offer = last<relation::step::offer>(graph);
    auto&& scan = next<relation::scan>(offer.input());
    {
        auto&& p0 = find(c.execution_plan(), scan);
        auto&& p1 = find(c.execution_plan(), offer);
        ASSERT_EQ(p0, p1);
    }

    auto& b2 = next_top(c.execution_plan(), b);
    auto&& graph2 = takatori::util::downcast<takatori::plan::process>(b2).operators();
    auto&& offer2 = last<relation::step::offer>(graph2);
    auto&& scan2 = next<relation::scan>(offer2.input());
    {
        auto&& p0 = find(c.execution_plan(), scan2);
        auto&& p1 = find(c.execution_plan(), offer2);
        ASSERT_EQ(p0, p1);
    }

    auto& grp1 = b.downstreams()[0];
    auto& grp2 = b2.downstreams()[0];

    auto mirrors = std::make_shared<mirror_container>();
    jogasaki::plan::impl::preprocess(b, info, mirrors);
    auto s = jogasaki::plan::impl::create(b, info, mirrors, nullptr);
    auto io_map = s.relation_io_map();
    ASSERT_EQ(0, io_map->output_index(bindings(grp1)));

    auto& b3 = grp1.downstreams()[0];
    auto&& graph3 = takatori::util::downcast<takatori::plan::process>(b3).operators();
    auto&& emit = last<relation::emit>(graph3);
    auto&& project = next<relation::project>(emit.input());
    auto&& join = next<relation::step::join>(project.input());
    auto&& take = next<relation::step::take_cogroup>(join.input());
    {
        auto&& p0 = find(c.execution_plan(), take);
        auto&& p1 = find(c.execution_plan(), join);
        auto&& p2 = find(c.execution_plan(), emit);
        auto&& p3 = find(c.execution_plan(), project);
        ASSERT_EQ(p0, p1);
        ASSERT_EQ(p1, p2);
        ASSERT_EQ(p2, p3);
        {
            // some experiments
            auto g0 = take.groups()[0];
            auto src_c0 = g0.columns()[0].source();
            auto dest_c0 = g0.columns()[0].destination();
            auto& resolved = info.type_of(dest_c0);
        }
    }
}

TEST_F(compiler_test, aggregate) {
    std::string sql = "select sum(T0.C1), T0.C0 from T0 group by C0";
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ctx.aggregate_provider(aggregate_functions());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info =ctx.executable_statement()->compiled_info();
    auto& stmt =*ctx.executable_statement()->statement();
    auto&& c = downcast<statement::execute>(stmt);
   dump(info, stmt);

    ASSERT_EQ(c.execution_plan().size(), 3);

    auto& b = top(c.execution_plan());
    auto&& graph = takatori::util::downcast<takatori::plan::process>(b).operators();
    auto&& offer = last<relation::step::offer>(graph);
    auto&& project0 = next<relation::project>(offer.input());
    auto&& scan = next<relation::scan>(project0.input());
    {
        auto&& p0 = find(c.execution_plan(), scan);
        auto&& p1 = find(c.execution_plan(), offer);
        auto&& p2 = find(c.execution_plan(), project0);
        ASSERT_EQ(p0, p1);
        ASSERT_EQ(p1, p2);
    }

    auto& agg = b.downstreams()[0];

    auto mirrors = std::make_shared<mirror_container>();
    jogasaki::plan::impl::preprocess(b, info, mirrors);
    auto s = jogasaki::plan::impl::create(b, info, mirrors, nullptr);
    auto io_map = s.relation_io_map();
    ASSERT_EQ(0, io_map->output_index(bindings(agg)));

    auto& b3 = agg.downstreams()[0];
    auto&& graph3 = takatori::util::downcast<takatori::plan::process>(b3).operators();
    auto&& emit = last<relation::emit>(graph3);
    auto&& project1 = next<relation::project>(emit.input());
    auto&& flatten = next<relation::step::flatten>(project1.input());
    auto&& take = next<relation::step::take_group>(flatten.input());
    {
        auto&& p0 = find(c.execution_plan(), take);
        auto&& p1 = find(c.execution_plan(), flatten);
        auto&& p2 = find(c.execution_plan(), emit);
        auto&& p3 = find(c.execution_plan(), project1);
        ASSERT_EQ(p0, p1);
        ASSERT_EQ(p1, p2);
        ASSERT_EQ(p2, p3);
    }
}

}
