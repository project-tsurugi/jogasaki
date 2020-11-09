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
#include <jogasaki/executor/process/impl/ops/offer.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/offer_context.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

class offer_test : public test_root {};

TEST_F(offer_test, simple) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table("T0", {
        "T0",
        {
            { "C0", t::int4() },
            { "C1", t::float8() },
            { "C2", t::int8() },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<storage::index> i0 = storages->add_index("I0", { t0, "I0", });

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
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");
    auto& r0 = p0.operators().insert(relation::scan {
        bindings(*i0),
        {
            { bindings(t0c0), c0 },
            { bindings(t0c1), c1 },
            { bindings(t0c2), c2 },
        },
    });

    auto&& r1 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
        },
    });

    r0.output() >> r1.input();

    auto vm = std::make_shared<yugawara::analyzer::variable_mapping>();
    vm->bind(c0, t::int4{});
    vm->bind(c1, t::float8{});
    vm->bind(c2, t::int8{});
    vm->bind(f1c0, t::int4{});
    vm->bind(f1c1, t::float8{});
    vm->bind(f1c2, t::int8{});
    vm->bind(bindings(t0c0), t::int4{});
    vm->bind(bindings(t0c1), t::float8{});
    vm->bind(bindings(t0c2), t::int8{});
    yugawara::compiled_info c_info{{}, vm};

    processor_info p_info{p0.operators(), c_info};

    // currently this vector order defines the order of variables
    // TODO fix when the logic is fixed
    std::vector<variable> columns{f1c1, f1c0, f1c2};
    variable_order order{
        variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
        columns
    };

    std::vector<offer::column> offer_columns{
        {c0, f1c0},
        {c1, f1c1},
        {c2, f1c2},
    };

    using kind = meta::field_type_kind;
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::float8>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{"000"s}
    );
    offer s{
        0,
        p_info, 0,
        order,
        meta,
        offer_columns,
        0
    };

    ASSERT_EQ(1, p_info.scopes_info().size());
    auto& block_info = p_info.scopes_info()[s.block_index()];
    block_scope variables{block_info};

    using kind = meta::field_type_kind;
    using test_record = jogasaki::mock::basic_record<kind::float8, kind::int4, kind::int8>;
    auto writer = std::make_shared<mock::basic_record_writer<test_record>>(s.meta());

    mock::task_context task_ctx{
        {},
        {writer},
        {},
        {},
    };

    offer_context ctx(&task_ctx, s.meta(), variables);

    auto vars_ref = variables.store().ref();
    auto map = variables.value_map();
    auto vars_meta = variables.meta();
    vars_ref.set_value<std::int32_t>(map.at(c0).value_offset(), 0);
    vars_ref.set_value<double>(map.at(c1).value_offset(), 1.0);
    vars_ref.set_value<std::int64_t>(map.at(c2).value_offset(), 2);
    s(ctx);
    auto internal_cols_ref = ctx.store().ref();
    auto& internal_cols_meta = s.meta();
    EXPECT_EQ(1.0, internal_cols_ref.get_value<double>(internal_cols_meta->value_offset(0)));
    EXPECT_EQ(0, internal_cols_ref.get_value<std::int32_t>(internal_cols_meta->value_offset(1)));
    EXPECT_EQ(2, internal_cols_ref.get_value<std::int64_t>(internal_cols_meta->value_offset(2)));

    ASSERT_EQ(1, writer->size());
    auto& records = writer->records();

    test_record exp{1.0, 0, 2};
    EXPECT_EQ(exp, records[0]);

    vars_ref.set_value<std::int32_t>(map.at(c0).value_offset(), 3);
    s(ctx);
    ASSERT_EQ(2, writer->size());
    test_record exp2{1.0, 3, 2};
    EXPECT_EQ(exp2, records[1]);
}

}

