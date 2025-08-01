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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/expression.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/infect_qualifier.h>
#include <takatori/value/character.h>
#include <takatori/value/primitive.h>
#include <takatori/value/value_kind.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/process/impl/ops/project.h>
#include <jogasaki/executor/process/impl/ops/project_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

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

class project_test : public test_root {
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
    inline immediate constant_text(std::string_view v, type::data&& type = type::character(type::varying, 64)) {
        return immediate { value::character(v), std::move(type) };
    }
    std::shared_ptr<yugawara::analyzer::variable_mapping> variables_ = std::make_shared<yugawara::analyzer::variable_mapping>();
    std::shared_ptr<yugawara::analyzer::expression_mapping> expressions_ = std::make_shared<yugawara::analyzer::expression_mapping>();
};

TEST_F(project_test, simple) {
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
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    auto&& f1c3 = f1.columns()[3];
    auto&& f1c4 = f1.columns()[4];

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
    auto expr = std::make_unique<binary>(
        binary_operator::add,
        varref(c1),
        binary {
            binary_operator::add,
            varref(c2),
            constant(1)
        }
    );
    expressions().bind(*expr, t::int8 {});
    expressions().bind(expr->left(), t::int8 {});
    expressions().bind(expr->right(), t::int8 {});
    auto& r = static_cast<binary&>(expr->right());
    expressions().bind(r.left(), t::int8 {});
    expressions().bind(r.right(), t::int8 {});

    std::vector<relation::project::column> columns{};
    // use emplace to avoid copying expr, whose parts have been registered by bind() above
    using column = relation::project::column;
    auto c3 = bindings.stream_variable("C3");
    auto c4 = bindings.stream_variable("C4");

    std::vector<column> v{};
    v.emplace_back(
        relation::project::column {
            c3,
            constant(100),
        }
    );
    expressions().bind(v[0].value(), t::int8 {});
    v.emplace_back(
        relation::project::column {
            c4,
            std::move(expr)
        }
    );
    auto& r1 = p0.operators().emplace<relation::project>(std::move(v));

    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
            { c3, f1c3 },
            { c4, f1c4 },
        },
    });

    r0.output() >> r1.input();
    r1.output() >> r2.input();

    variables().bind(c0, t::int8{});
    variables().bind(c1, t::int8{});
    variables().bind(c2, t::int8{});
    variables().bind(c3, t::int8{});
    variables().bind(c4, t::int8{});
    variables().bind(f1c0, t::int8{});
    variables().bind(f1c1, t::int8{});
    variables().bind(f1c2, t::int8{});
    variables().bind(f1c3, t::int8{});
    variables().bind(f1c4, t::int8{});
    variables().bind(bindings(t0c0), t::int8{});
    variables().bind(bindings(t0c1), t::int8{});
    variables().bind(bindings(t0c2), t::int8{});

    yugawara::compiled_info c_info{expressions_, variables_};
    processor_info p_info{p0.operators(), c_info};

    relation::project downstream{};
    project s{
        0,
        p_info,
        0,
        r1.columns()
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
    project_context ctx(&task_ctx, variables, &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();
    vars_ref.set_value<std::int64_t>(map.at(c0).value_offset(), 1);
    vars_ref.set_value<std::int64_t>(map.at(c1).value_offset(), 11);
    vars_ref.set_value<std::int64_t>(map.at(c2).value_offset(), 10);
    vars_ref.set_null(map.at(c0).nullity_offset(), false);
    vars_ref.set_null(map.at(c1).nullity_offset(), false);
    vars_ref.set_null(map.at(c2).nullity_offset(), false);
    s(ctx);

    ASSERT_EQ(100, vars_ref.get_value<std::int64_t>(map.at(c3).value_offset()));
    ASSERT_EQ(22, vars_ref.get_value<std::int64_t>(map.at(c4).value_offset()));
}

TEST_F(project_test, text) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::character(type::varying, 64) },
            { "C1", t::character(type::varying, 64) },
            { "C2", t::character(type::varying, 64) },
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
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    auto&& f1c3 = f1.columns()[3];
    auto&& f1c4 = f1.columns()[4];

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
    auto expr1 = std::make_unique<binary>(
        binary_operator::concat,
        varref(c1),
        binary {
            binary_operator::concat,
            varref(c2),
            constant_text("Z23456789012345678901234567890")
        }
    );
    expressions().bind(*expr1, t::character(type::varying, 64+64+64));
    expressions().bind(expr1->left(), t::character(type::varying, 64));
    expressions().bind(expr1->right(), t::character(type::varying, 64+64));
    auto& r = static_cast<binary&>(expr1->right());
    expressions().bind(r.left(), t::character(type::varying, 64));
    expressions().bind(r.right(), t::character(type::varying, 64));

    std::vector<relation::project::column> columns{};
    // use emplace to avoid copying expr, whose parts have been registered by bind() above
    using column = relation::project::column;
    auto c3 = bindings.stream_variable("C3");

    std::vector<column> v{};
    v.emplace_back(
        relation::project::column {
            c3,
            std::move(expr1)
        }
    );
    expressions().bind(v[0].value(), t::character(type::varying, 64+64+64));
    auto& r1 = p0.operators().emplace<relation::project>(std::move(v));

    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
            { c3, f1c3 },
        },
    });

    r0.output() >> r1.input();
    r1.output() >> r2.input();

    variables().bind(c0, t::character{type::varying, 64});
    variables().bind(c1, t::character{type::varying, 64});
    variables().bind(c2, t::character{type::varying, 64});
    variables().bind(c3, t::character{type::varying, 64+64+64});
    variables().bind(f1c0, t::character{type::varying, 64});
    variables().bind(f1c1, t::character{type::varying, 64});
    variables().bind(f1c2, t::character{type::varying, 64});
    variables().bind(f1c3, t::character{type::varying, 64+64+64});
    variables().bind(bindings(t0c0), t::character{type::varying, 64});
    variables().bind(bindings(t0c1), t::character{type::varying, 64});
    variables().bind(bindings(t0c2), t::character{type::varying, 64});

    yugawara::compiled_info c_info{expressions_, variables_};
    processor_info p_info{p0.operators(), c_info};

    relation::project downstream{};
    project s{
        0,
        p_info,
        0,
        r1.columns()
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
    memory::lifo_paged_memory_resource res{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&pool};
    project_context ctx(&task_ctx, variables, &res, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();

    vars_ref.set_value<text>(map.at(c0).value_offset(), text{&res, "A23456789012345678901234567890"});
    vars_ref.set_value<text>(map.at(c1).value_offset(), text{&res, "B23456789012345678901234567890"});
    vars_ref.set_value<text>(map.at(c2).value_offset(), text{&res, "C23456789012345678901234567890"});
    vars_ref.set_null(map.at(c0).nullity_offset(), false);
    vars_ref.set_null(map.at(c1).nullity_offset(), false);
    vars_ref.set_null(map.at(c2).nullity_offset(), false);
    s(ctx);
    text exp{&res, "B23456789012345678901234567890C23456789012345678901234567890Z23456789012345678901234567890"};
    ASSERT_EQ(exp, vars_ref.get_value<text>(map.at(c3).value_offset()));
}

}

