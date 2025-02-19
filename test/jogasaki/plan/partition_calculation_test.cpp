/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <gtest/gtest.h>
#include <string>
#include <vector>

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
#include <jogasaki/test_utils.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::plan {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

namespace type      = ::takatori::type;
namespace value     = ::takatori::value;
namespace scalar    = ::takatori::scalar;
namespace relation  = ::takatori::relation;
namespace statement = ::takatori::statement;

using namespace testing;

using namespace ::yugawara::variable;
using namespace ::yugawara;
/**
 * @brief test to confirm the compiler behavior
 * TOOO this is temporary, do not depend on compiler to generate same plan
 */
class partition_calculation_test : public ::testing::Test {
  public:
    using kind = field_type_kind;
    ::yugawara::binding::factory bindings{};

    std::shared_ptr<::yugawara::storage::configurable_provider> tables() {

        std::shared_ptr<::yugawara::storage::configurable_provider> storages =
            std::make_shared<::yugawara::storage::configurable_provider>();

        std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table({
            "T0",
            {
                {"C0", type::int8(), criteria{nullity{false}}},
                {"C1", type::float8()},
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
                {"C0", type::int8(), criteria{nullity{false}}},
                {"C1", type::float8()},
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
        executor::function::incremental::add_builtin_aggregate_functions(
            *provider, global::incremental_aggregate_function_repository());
        return provider;
    }
};

TEST_F(partition_calculation_test, simple_query_rtx) {
    const int parallel  = 3;
    const int partition = 7;
    std::string sql     = "select * from T0";
    compiler_context ctx{};
    auto cfg = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);
    ASSERT_EQ(c.execution_plan().size(), 1);
    auto&& p0 = top(c.execution_plan());
    EXPECT_TRUE(impl::has_emit_operator(p0));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(parallel, impl::calculate_partition(p0));
}
TEST_F(partition_calculation_test, simple_query_no_rtx) {
    std::string sql     = "select * from T0";
    const int parallel  = 3;
    const int partition = 7;
    compiler_context ctx{};
    auto cfg = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(false);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);
    ASSERT_EQ(c.execution_plan().size(), 1);
    auto&& p0 = top(c.execution_plan());
    EXPECT_TRUE(impl::has_emit_operator(p0));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(1, impl::calculate_partition(p0));
}

TEST_F(partition_calculation_test, simple_query2_rtx) {
    std::string sql     = "select * from T0 where C1 = 1.0;";
    const int parallel  = 3;
    const int partition = 7;
    compiler_context ctx{};
    auto cfg = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);
    ASSERT_EQ(c.execution_plan().size(), 1);
    auto&& p0 = top(c.execution_plan());
    EXPECT_TRUE(impl::has_emit_operator(p0));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(parallel, impl::calculate_partition(p0));
}
TEST_F(partition_calculation_test, simple_query2_no_rtx) {
    std::string sql     = "select * from T0 where C1 = 1.0;";
    const int parallel  = 3;
    const int partition = 7;
    compiler_context ctx{};
    auto cfg = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(false);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);
    ASSERT_EQ(c.execution_plan().size(), 1);
    auto&& p0 = top(c.execution_plan());
    EXPECT_TRUE(impl::has_emit_operator(p0));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(1, impl::calculate_partition(p0));
}

TEST_F(partition_calculation_test, project_filter_rtx) {
    std::string sql     = "select C1+C0, C0, C1 from T0 where C1=1.0";
    const int parallel  = 3;
    const int partition = 7;
    auto cfg            = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);

    ASSERT_EQ(c.execution_plan().size(), 1);

    auto b         = c.execution_plan().begin();
    auto&& graph   = takatori::util::downcast<takatori::plan::process>(*b).operators();
    auto&& emit    = last<relation::emit>(graph);
    auto&& project = next<relation::project>(emit.input());
    auto&& filter  = next<relation::filter>(project.input());
    auto&& scan    = next<relation::scan>(filter.input());

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
    EXPECT_TRUE(impl::has_emit_operator(p0));
    EXPECT_TRUE(impl::has_emit_operator(p1));
    EXPECT_TRUE(impl::has_emit_operator(p2));
    EXPECT_TRUE(impl::has_emit_operator(p3));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p1));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p2));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p3));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p1));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p2));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p3));
    EXPECT_EQ(parallel, impl::calculate_partition(p0));
    EXPECT_EQ(parallel, impl::calculate_partition(p1));
    EXPECT_EQ(parallel, impl::calculate_partition(p2));
    EXPECT_EQ(parallel, impl::calculate_partition(p3));
}
TEST_F(partition_calculation_test, project_filter_no_rtx) {
    std::string sql     = "select C1+C0, C0, C1 from T0 where C1=1.0";
    const int parallel  = 3;
    const int partition = 7;
    auto cfg            = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(false);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);

    ASSERT_EQ(c.execution_plan().size(), 1);

    auto b         = c.execution_plan().begin();
    auto&& graph   = takatori::util::downcast<takatori::plan::process>(*b).operators();
    auto&& emit    = last<relation::emit>(graph);
    auto&& project = next<relation::project>(emit.input());
    auto&& filter  = next<relation::filter>(project.input());
    auto&& scan    = next<relation::scan>(filter.input());

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
    EXPECT_TRUE(impl::has_emit_operator(p0));
    EXPECT_TRUE(impl::has_emit_operator(p1));
    EXPECT_TRUE(impl::has_emit_operator(p2));
    EXPECT_TRUE(impl::has_emit_operator(p3));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p1));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p2));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p3));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p1));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p2));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p3));
    EXPECT_EQ(1, impl::calculate_partition(p0));
    EXPECT_EQ(1, impl::calculate_partition(p1));
    EXPECT_EQ(1, impl::calculate_partition(p2));
    EXPECT_EQ(1, impl::calculate_partition(p3));
}

TEST_F(partition_calculation_test, left_outer_join_rtx) {
    const int parallel  = 3;
    const int partition = 7;
    std::string sql     = "select T0.C0, T1.C1 from T0 LEFT OUTER JOIN T1 ON T0.C1 = T1.C1";
    auto cfg            = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);

    ASSERT_EQ(c.execution_plan().size(), 5);

    auto& b      = top(c.execution_plan());
    auto&& graph = takatori::util::downcast<takatori::plan::process>(b).operators();
    auto&& offer = last<relation::step::offer>(graph);
    auto&& scan  = next<relation::scan>(offer.input());
    {
        auto&& p0 = find(c.execution_plan(), scan);
        auto&& p1 = find(c.execution_plan(), offer);
        ASSERT_EQ(p0, p1);
        EXPECT_FALSE(impl::has_emit_operator(p0));
        EXPECT_FALSE(impl::has_emit_operator(p1));
        EXPECT_EQ(parallel, impl::terminal_calculate_partition(p0));
        EXPECT_EQ(parallel, impl::terminal_calculate_partition(p1));
        EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p0));
        EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p1));
        EXPECT_EQ(partition, impl::calculate_partition(p0));
        EXPECT_EQ(partition, impl::calculate_partition(p1));
    }
    auto& b2      = next_top(c.execution_plan(), b);
    auto&& graph2 = takatori::util::downcast<takatori::plan::process>(b2).operators();
    auto&& offer2 = last<relation::step::offer>(graph2);
    auto&& scan2  = next<relation::scan>(offer2.input());
    {
        auto&& p0 = find(c.execution_plan(), scan2);
        auto&& p1 = find(c.execution_plan(), offer2);
        ASSERT_EQ(p0, p1);
        ASSERT_EQ(p0, p1);
        EXPECT_FALSE(impl::has_emit_operator(p0));
        EXPECT_FALSE(impl::has_emit_operator(p1));
        EXPECT_EQ(parallel, impl::terminal_calculate_partition(p0));
        EXPECT_EQ(parallel, impl::terminal_calculate_partition(p1));
        EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p0));
        EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p1));
        EXPECT_EQ(partition, impl::calculate_partition(p0));
        EXPECT_EQ(partition, impl::calculate_partition(p1));
    }
    auto& grp1 = b.downstreams()[0];
    auto& grp2 = b2.downstreams()[0];

    auto mirrors = std::make_shared<mirror_container>();
    jogasaki::plan::impl::preprocess(b, info, mirrors);
    auto s      = jogasaki::plan::impl::create(b, info, mirrors, nullptr);
    auto io_map = s.relation_io_map();
    ASSERT_EQ(0, io_map->output_index(bindings(grp1)));

    auto& b3       = grp1.downstreams()[0];
    auto&& graph3  = takatori::util::downcast<takatori::plan::process>(b3).operators();
    auto&& emit    = last<relation::emit>(graph3);
    auto&& project = next<relation::project>(emit.input());
    auto&& join    = next<relation::step::join>(project.input());
    auto&& take    = next<relation::step::take_cogroup>(join.input());
    {
        auto&& p0 = find(c.execution_plan(), take);
        auto&& p1 = find(c.execution_plan(), join);
        auto&& p2 = find(c.execution_plan(), emit);
        auto&& p3 = find(c.execution_plan(), project);
        ASSERT_EQ(p0, p1);
        ASSERT_EQ(p1, p2);
        ASSERT_EQ(p2, p3);
        EXPECT_TRUE(impl::has_emit_operator(p0));
        EXPECT_TRUE(impl::has_emit_operator(p1));
        EXPECT_TRUE(impl::has_emit_operator(p2));
        EXPECT_TRUE(impl::has_emit_operator(p3));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p0));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p1));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p2));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p3));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p0));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p1));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p2));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p3));
        EXPECT_EQ(partition, impl::calculate_partition(p0));
        EXPECT_EQ(partition, impl::calculate_partition(p1));
        EXPECT_EQ(partition, impl::calculate_partition(p2));
        EXPECT_EQ(partition, impl::calculate_partition(p3));
    }
}

TEST_F(partition_calculation_test, left_outer_join_no_rtx) {
    const int parallel  = 3;
    const int partition = 7;
    std::string sql     = "select T0.C0, T1.C1 from T0 LEFT OUTER JOIN T1 ON T0.C1 = T1.C1";
    auto cfg            = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(false);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    compiler_context ctx{};
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);

    ASSERT_EQ(c.execution_plan().size(), 5);

    auto& b      = top(c.execution_plan());
    auto&& graph = takatori::util::downcast<takatori::plan::process>(b).operators();
    auto&& offer = last<relation::step::offer>(graph);
    auto&& scan  = next<relation::scan>(offer.input());
    {
        auto&& p0 = find(c.execution_plan(), scan);
        auto&& p1 = find(c.execution_plan(), offer);
        ASSERT_EQ(p0, p1);
        EXPECT_FALSE(impl::has_emit_operator(p0));
        EXPECT_FALSE(impl::has_emit_operator(p1));
        EXPECT_EQ(1, impl::terminal_calculate_partition(p0));
        EXPECT_EQ(1, impl::terminal_calculate_partition(p1));
        EXPECT_EQ(1, impl::intermediate_calculate_partition(p0));
        EXPECT_EQ(1, impl::intermediate_calculate_partition(p1));
        EXPECT_EQ(partition, impl::calculate_partition(p0));
        EXPECT_EQ(partition, impl::calculate_partition(p1));
    }
    auto& b2      = next_top(c.execution_plan(), b);
    auto&& graph2 = takatori::util::downcast<takatori::plan::process>(b2).operators();
    auto&& offer2 = last<relation::step::offer>(graph2);
    auto&& scan2  = next<relation::scan>(offer2.input());
    {
        auto&& p0 = find(c.execution_plan(), scan2);
        auto&& p1 = find(c.execution_plan(), offer2);
        ASSERT_EQ(p0, p1);
        ASSERT_EQ(p0, p1);
        EXPECT_FALSE(impl::has_emit_operator(p0));
        EXPECT_FALSE(impl::has_emit_operator(p1));
        EXPECT_EQ(1, impl::terminal_calculate_partition(p0));
        EXPECT_EQ(1, impl::terminal_calculate_partition(p1));
        EXPECT_EQ(1, impl::intermediate_calculate_partition(p0));
        EXPECT_EQ(1, impl::intermediate_calculate_partition(p1));
        EXPECT_EQ(partition, impl::calculate_partition(p0));
        EXPECT_EQ(partition, impl::calculate_partition(p1));
    }
    auto& grp1 = b.downstreams()[0];
    auto& grp2 = b2.downstreams()[0];

    auto mirrors = std::make_shared<mirror_container>();
    jogasaki::plan::impl::preprocess(b, info, mirrors);
    auto s      = jogasaki::plan::impl::create(b, info, mirrors, nullptr);
    auto io_map = s.relation_io_map();
    ASSERT_EQ(0, io_map->output_index(bindings(grp1)));

    auto& b3       = grp1.downstreams()[0];
    auto&& graph3  = takatori::util::downcast<takatori::plan::process>(b3).operators();
    auto&& emit    = last<relation::emit>(graph3);
    auto&& project = next<relation::project>(emit.input());
    auto&& join    = next<relation::step::join>(project.input());
    auto&& take    = next<relation::step::take_cogroup>(join.input());
    {
        auto&& p0 = find(c.execution_plan(), take);
        auto&& p1 = find(c.execution_plan(), join);
        auto&& p2 = find(c.execution_plan(), emit);
        auto&& p3 = find(c.execution_plan(), project);
        ASSERT_EQ(p0, p1);
        ASSERT_EQ(p1, p2);
        ASSERT_EQ(p2, p3);
        EXPECT_TRUE(impl::has_emit_operator(p0));
        EXPECT_TRUE(impl::has_emit_operator(p1));
        EXPECT_TRUE(impl::has_emit_operator(p2));
        EXPECT_TRUE(impl::has_emit_operator(p3));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p0));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p1));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p2));
        EXPECT_EQ(partition, impl::terminal_calculate_partition(p3));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p0));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p1));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p2));
        EXPECT_EQ(partition, impl::intermediate_calculate_partition(p3));
        EXPECT_EQ(partition, impl::calculate_partition(p0));
        EXPECT_EQ(partition, impl::calculate_partition(p1));
        EXPECT_EQ(partition, impl::calculate_partition(p2));
        EXPECT_EQ(partition, impl::calculate_partition(p3));
    }
}

TEST_F(partition_calculation_test, union_all_rtx) {
    std::string sql     = "select * from T0 union all select * from T1";
    const int parallel  = 3;
    const int partition = 7;
    compiler_context ctx{};
    auto cfg = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);
    ASSERT_EQ(c.execution_plan().size(), 4);
    auto&& p0 = top(c.execution_plan());
    EXPECT_FALSE(impl::has_emit_operator(p0));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(partition, impl::calculate_partition(p0));
    auto& p1 = next_top(c.execution_plan(), p0);
    EXPECT_FALSE(impl::has_emit_operator(p1));
    EXPECT_EQ(parallel, impl::terminal_calculate_partition(p1));
    EXPECT_EQ(parallel, impl::intermediate_calculate_partition(p1));
    EXPECT_EQ(partition, impl::calculate_partition(p1));

    auto& grp1    = p0.downstreams()[0];
    auto& b3      = grp1.downstreams()[0];
    auto&& graph3 = takatori::util::downcast<takatori::plan::process>(b3).operators();
    auto&& emit   = last<relation::emit>(graph3);
    auto&& p2     = find(c.execution_plan(), emit);
    EXPECT_TRUE(impl::has_emit_operator(p2));
    EXPECT_EQ(partition, impl::terminal_calculate_partition(p2));
    EXPECT_EQ(parallel * 2, impl::intermediate_calculate_partition(p2));
    EXPECT_EQ(parallel * 2, impl::calculate_partition(p2));
}

TEST_F(partition_calculation_test, union_all_no_rtx) {
    std::string sql     = "select * from T0 union all select * from T1";
    const int parallel  = 3;
    const int partition = 7;
    compiler_context ctx{};
    auto cfg = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(false);
    cfg->scan_default_parallel(parallel);
    cfg->default_partitions(partition);
    global::config_pool(cfg);
    ctx.storage_provider(tables());
    ASSERT_EQ(status::ok, compile(sql, ctx));
    auto& info = ctx.executable_statement()->compiled_info();
    auto& stmt = *ctx.executable_statement()->statement();
    auto&& c   = downcast<statement::execute>(stmt);
    ASSERT_EQ(c.execution_plan().size(), 4);
    auto&& p0 = top(c.execution_plan());
    EXPECT_FALSE(impl::has_emit_operator(p0));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p0));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p0));
    EXPECT_EQ(partition, impl::calculate_partition(p0));
    auto& p1 = next_top(c.execution_plan(), p0);
    EXPECT_FALSE(impl::has_emit_operator(p1));
    EXPECT_EQ(1, impl::terminal_calculate_partition(p1));
    EXPECT_EQ(1, impl::intermediate_calculate_partition(p1));
    EXPECT_EQ(partition, impl::calculate_partition(p1));

    auto& grp1    = p0.downstreams()[0];
    auto& b3      = grp1.downstreams()[0];
    auto&& graph3 = takatori::util::downcast<takatori::plan::process>(b3).operators();
    auto&& emit   = last<relation::emit>(graph3);
    auto&& p2     = find(c.execution_plan(), emit);
    EXPECT_TRUE(impl::has_emit_operator(p2));
    EXPECT_EQ(partition, impl::terminal_calculate_partition(p2));
    EXPECT_EQ(2, impl::intermediate_calculate_partition(p2));
    EXPECT_EQ(2, impl::calculate_partition(p2));
}

} // namespace jogasaki::plan
