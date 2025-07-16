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
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/element.h>
#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/aggregate.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_group.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/data.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/exception.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/value/value_kind.h>
#include <yugawara/aggregate/basic_configurable_provider.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/aggregate/declaration.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/binding/variable_info_kind.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/relation_kind.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/data/value_store.h>
#include <jogasaki/executor/function/aggregate_function_kind.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/impl/ops/aggregate_group.h>
#include <jogasaki/executor/process/impl/ops/aggregate_group_context.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

#include "verifier.h"

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;
using namespace yugawara::binding;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace statement = ::takatori::statement;
namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;

using kind = meta::field_type_kind;

class aggregate_group_test : public test_root {
public:

    yugawara::analyzer::variable_mapping& variables() noexcept {
        return *variables_;
    }

    yugawara::analyzer::expression_mapping& expressions() noexcept {
        return *expressions_;
    }

    std::shared_ptr<yugawara::analyzer::variable_mapping> variables_ = std::make_shared<yugawara::analyzer::variable_mapping>();
    std::shared_ptr<yugawara::analyzer::expression_mapping> expressions_ = std::make_shared<yugawara::analyzer::expression_mapping>();
};

TEST_F(aggregate_group_test, simple) {
    binding::factory bindings;

    std::shared_ptr<::yugawara::aggregate::configurable_provider> functions
        = std::make_shared<::yugawara::aggregate::configurable_provider>();
    function::add_builtin_aggregate_functions(*functions, global::aggregate_function_repository());

    auto&& g0c0 = bindings.exchange_column("g0c0");
    auto&& g0c1 = bindings.exchange_column("g0c1");
    auto&& g0c2 = bindings.exchange_column("g0c2");
    ::takatori::plan::group g0{
        {
            g0c0,
            g0c1,
            g0c2,
        },
        {
            g0c0,
        },
    };
    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");
    auto& r0 = p0.operators().insert(relation::step::take_group {
        bindings(g0),
        {
            { g0c0, c0 },
            { g0c1, c1 },
            { g0c2, c2 },
        },
    });

    ::takatori::plan::forward f1 {
        bindings.exchange_column("f1c0"),
        bindings.exchange_column("f1c1"),
        bindings.exchange_column("f1c2"),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    // without offer, the columns are not used and block variables become empty

    yugawara::aggregate::declaration const* f_int8{};
    yugawara::aggregate::declaration const* f_float8{};
    functions->each([&](std::shared_ptr<yugawara::aggregate::declaration const> const& decl) {
        if(decl->name() == "count$distinct") {
            if(decl->parameter_types()[0] == t::int8()) {
                f_int8 = decl.get();
            } else if(decl->parameter_types()[0] == t::float8()) {
                f_float8 = decl.get();
            }
        }
    });
    auto func0 = bindings(std::move(const_cast<yugawara::aggregate::declaration&>(*f_int8)));
    auto func1 = bindings(std::move(const_cast<yugawara::aggregate::declaration&>(*f_float8)));
    auto rc1 = bindings.stream_variable("rc1");
    auto rc2 = bindings.stream_variable("rc2");
    auto&& r1 = p0.operators().insert(relation::step::aggregate {
        {
            {
                func0,
                { c1 },
                rc1
            },
            {
                func0,
                { c2 },
                rc2
            }
        },
    });
    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { rc1, f1c1 },
            { rc2, f1c2 },
        },
    });

    r0.output() >> r1.input();
    r1.output() >> r2.input();

    variables().bind(c0, t::int8{});
    variables().bind(c1, t::int8{});
    variables().bind(c2, t::int8{});
    variables().bind(f1c0, t::int8{});
    variables().bind(f1c1, t::int8{});
    variables().bind(f1c2, t::int8{});
    variables().bind(rc1, t::int8{});
    variables().bind(rc2, t::int8{});

    yugawara::compiled_info c_info{expressions_, variables_};
    processor_info p_info{p0.operators(), c_info};

    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    aggregate_group s{
        0,
        p_info,
        0,
        r1.columns(),
        std::move(d)
    };

    ASSERT_EQ(1, p_info.vars_info_list().size());
    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&pool};

    std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> resources{};
    resources.emplace_back(std::make_unique<memory::lifo_paged_memory_resource>(&pool));
    resources.emplace_back(std::make_unique<memory::lifo_paged_memory_resource>(&pool));
    std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> nulls_resources{};
    nulls_resources.emplace_back(std::make_unique<memory::lifo_paged_memory_resource>(&pool));
    nulls_resources.emplace_back(std::make_unique<memory::lifo_paged_memory_resource>(&pool));
    std::vector<data::value_store> stores{};
    stores.emplace_back(
        meta::field_type{field_enum_tag<kind::int8>},
        resources[0].get(),
        &varlen_resource,
        nulls_resources[0].get()
    );
    stores.emplace_back(
        meta::field_type{field_enum_tag<kind::int8>},
        resources[1].get(),
        &varlen_resource,
        nulls_resources[1].get()
    );
    std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores{
        {
            stores[0],
        },
        {
            stores[1],
        }
    };
    aggregate_group_context ctx{
        &task_ctx,
        variables,
        &resource,
        &varlen_resource,
        std::move(stores),
        std::move(resources),
        std::move(function_arg_stores),
        std::move(nulls_resources)
    };

    std::vector<std::vector<std::int64_t>> test_values {
        {0, 1, 1},
        {0, 2, 2},
        {0, 2, 3},
        {1, 1, 1},
        {1, 2, 2},
        {1, 3, 2},
    };

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();
    std::size_t called = 0;
    downstream->body([&](){
        switch(called) {
            case 0:
                ASSERT_EQ(2, vars_ref.get_value<std::int64_t>(map.at(rc1).value_offset()));
                ASSERT_EQ(3, vars_ref.get_value<std::int64_t>(map.at(rc2).value_offset()));
                break;
            case 1:
                ASSERT_EQ(3, vars_ref.get_value<std::int64_t>(map.at(rc1).value_offset()));
                ASSERT_EQ(2, vars_ref.get_value<std::int64_t>(map.at(rc2).value_offset()));
                break;
            default:
                std::abort();
        }
        ++called;
    });

    std::size_t count = 0;
    for(auto&& values : test_values) {
        vars_ref.set_value<std::int64_t>(map.at(c0).value_offset(), values[0]);
        vars_ref.set_null(map.at(c0).nullity_offset(), false);
        vars_ref.set_value<std::int64_t>(map.at(c1).value_offset(), values[1]);
        vars_ref.set_null(map.at(c1).nullity_offset(), false);
        vars_ref.set_value<std::int64_t>(map.at(c2).value_offset(), values[2]);
        vars_ref.set_null(map.at(c2).nullity_offset(), false);
        bool last_member = count == 2 || count == 5;
        s(ctx, last_member);
        ++count;
    }
    ASSERT_EQ(2, called);
}

}

