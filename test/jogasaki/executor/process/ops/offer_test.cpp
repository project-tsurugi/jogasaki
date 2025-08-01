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
#include <memory>
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <boost/container/vector.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/variable.h>
#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/scan.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/type/primitive.h>
#include <takatori/util/exception.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/impl/ops/offer.h>
#include <jogasaki/executor/process/impl/ops/offer_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/mock/record_writer.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/mock/basic_record.h>
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

using namespace jogasaki::mock;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

class offer_test : public test_root {};

TEST_F(offer_test, simple) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
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
            field_type(field_enum_tag<kind::float8>),
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{3}.flip()
    );
    offer s{
        0,
        p_info, 0,
        order,
        meta,
        offer_columns,
        0
    };

    ASSERT_EQ(1, p_info.vars_info_list().size());
    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    using kind = meta::field_type_kind;
    using test_record = jogasaki::mock::basic_record;
    auto writer = std::make_shared<mock::basic_record_writer>(meta);

    mock::task_context task_ctx{
        {},
        {writer},
        {},
        {},
    };

    memory::lifo_paged_memory_resource resource{&global::page_pool()};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};
    offer_context ctx(&task_ctx, meta, variables, &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    vars_ref.set_value<std::int32_t>(map.at(c0).value_offset(), 0);
    vars_ref.set_null(map.at(c0).nullity_offset(), false);
    vars_ref.set_value<double>(map.at(c1).value_offset(), 1.0);
    vars_ref.set_null(map.at(c1).nullity_offset(), false);
    vars_ref.set_value<std::int64_t>(map.at(c2).value_offset(), 2);
    vars_ref.set_null(map.at(c2).nullity_offset(), false);
    s(ctx);
    auto internal_cols_ref = ctx.store().ref();
    EXPECT_EQ(1.0, internal_cols_ref.get_value<double>(meta->value_offset(0)));
    EXPECT_EQ(0, internal_cols_ref.get_value<std::int32_t>(meta->value_offset(1)));
    EXPECT_EQ(2, internal_cols_ref.get_value<std::int64_t>(meta->value_offset(2)));

    ASSERT_EQ(1, writer->size());
    auto& records = writer->records();

    EXPECT_EQ((create_nullable_record<kind::float8, kind::int4, kind::int8>(1.0, 0, 2)), records[0]);

    vars_ref.set_value<std::int32_t>(map.at(c0).value_offset(), 3);
    vars_ref.set_null(map.at(c0).nullity_offset(), false);
    s(ctx);
    ASSERT_EQ(2, writer->size());
    test_record exp2{create_nullable_record<kind::float8, kind::int4, kind::int8>(1.0, 3, 2)};
    EXPECT_EQ(exp2, records[1]);
}

}

