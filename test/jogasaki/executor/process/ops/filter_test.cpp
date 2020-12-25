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
#include <jogasaki/executor/process/impl/ops/filter.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>
#include <takatori/relation/step/offer.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/offer_context.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>

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

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using unary = takatori::scalar::unary;
using unary_operator = takatori::scalar::unary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;

class filter_test : public test_root {
public:

    yugawara::analyzer::variable_mapping& variables() noexcept {
        return *variables_;
    }

    yugawara::analyzer::expression_mapping& expressions() noexcept {
        return *expressions_;
    }

    inline immediate constant(int v, type::data&& type = type::int8()) {
        return immediate { value::int8(v), std::move(type) };
    }
    std::shared_ptr<yugawara::analyzer::variable_mapping> variables_ = std::make_shared<yugawara::analyzer::variable_mapping>();
    std::shared_ptr<yugawara::analyzer::expression_mapping> expressions_ = std::make_shared<yugawara::analyzer::expression_mapping>();
};

TEST_F(filter_test, simple) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::int8() },
            { "C1", t::int8() },
            { "C2", t::int8() },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<storage::index> i0 = storages->add_index({ t0, "I0", });

    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("C0");
    auto c1 = bindings.stream_variable("C1");
    auto c2 = bindings.stream_variable("C2");
    auto& r0 = p0.operators().insert(relation::scan {
        bindings(*i0),
        {
            { bindings(t0c0), c0 },
            { bindings(t0c1), c1 },
            { bindings(t0c2), c2 },
        },
    });
    object_creator creator{};

    auto expr = creator.create_unique<compare>(
        comparison_operator::equal,
        varref(c1),
        binary {
            binary_operator::add,
            varref(c2),
            constant(1)
        }
    );
    expressions().bind(*expr, t::boolean {});
    expressions().bind(expr->left(), t::int8 {});
    expressions().bind(expr->right(), t::int8 {});
    auto& r = static_cast<binary&>(expr->right());
    expressions().bind(r.left(), t::int8 {});
    expressions().bind(r.right(), t::int8 {});

    // use emplace to avoid copying expr, whose parts have been registered by bind() above
    auto& r1 = p0.operators().emplace<relation::filter>(
        std::move(expr)
    );

    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
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
    variables().bind(bindings(t0c0), t::int8{});
    variables().bind(bindings(t0c1), t::int8{});
    variables().bind(bindings(t0c2), t::int8{});

    yugawara::compiled_info c_info{expressions_, variables_};
    processor_info p_info{p0.operators(), c_info};

    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    filter s{
        0,
        p_info,
        0,
        r1.condition(),
        std::move(d)
    };

    ASSERT_EQ(1, p_info.scopes_info().size());
    auto& block_info = p_info.scopes_info()[s.block_index()];
    block_scope variables{block_info};

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&pool};
    filter_context ctx(&task_ctx, variables, &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto map = variables.value_map();
    auto vars_meta = variables.meta();
    vars_ref.set_value<std::int64_t>(map.at(c0).value_offset(), 1);
    vars_ref.set_value<std::int64_t>(map.at(c1).value_offset(), 11);
    vars_ref.set_value<std::int64_t>(map.at(c2).value_offset(), 10);

    bool called = false;
    downstream->body([&]() {
        called = true;
    });
    s(ctx);
    ASSERT_TRUE(called);

    called = false;
    vars_ref.set_value<std::int64_t>(map.at(c0).value_offset(), 2);
    vars_ref.set_value<std::int64_t>(map.at(c1).value_offset(), 20);
    vars_ref.set_value<std::int64_t>(map.at(c2).value_offset(), 22);
    s(ctx);
    ASSERT_FALSE(called);
}

}

