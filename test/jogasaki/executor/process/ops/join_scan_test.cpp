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
#include <algorithm>
#include <cstddef>
#include <unordered_map>
#include <boost/container/container_fwd.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/join_find.h>
#include <takatori/relation/join_kind.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/type/primitive.h>
#include <takatori/util/exception.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/impl/ops/index_join.h>
#include <jogasaki/executor/process/impl/ops/index_join_context.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/group_reader.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/utils/from_endpoint.h>

#include "verifier.h"

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

using kind = field_type_kind;
using group_reader = mock::basic_group_reader;
using group_type = group_reader::group_type;
using keys_type = group_type::key_type;
using values_type = group_type::value_type;

using yugawara::variable::nullity;
using yugawara::variable::criteria;
using scalar::compare;
using scalar::comparison_operator;

class join_scan_test :
    public test_root,
    public kvs_test_base,
    public operator_test_utils {

    void SetUp() override {
        kvs_db_setup();
    }
    void TearDown() override {
        kvs_db_teardown();
    }
public:
};

std::pair<std::shared_ptr<mock::task_context>, std::shared_ptr<request_context>> create_task_context(
    std::vector<io::reader_container> readers = {},
    std::vector<std::shared_ptr<io::record_writer>> downstream_writers = {},
    std::shared_ptr<io::record_writer> external_writer = {},
    std::shared_ptr<abstract::range> range = {},
    std::shared_ptr<transaction_context> tx = {}
) {
    auto ret = std::make_shared<mock::task_context>(
        std::move(readers),
        std::move(downstream_writers),
        std::move(external_writer),
        std::move(range)
    );

    auto rctx = std::make_shared<request_context>();
    rctx->transaction(tx);
    ret->work_context(
        std::make_unique<impl::work_context>(rctx.get(), 0, 0, nullptr, nullptr, nullptr, tx, false, false)
    );
    return {ret, rctx};
}

TEST_F(join_scan_test, simple) {
    // join target t1 (c0 bigint, c1 bigint, primary key (c0, c1))
    // records:
    //  - (1, 100)
    //  - (1, 101)
    //  - (2, 200)
    //  - (3, 300)
    // join key: t1.C0
    // upstream `take` passes (1, 2) as pair of lower and upper (both bounds are prefixed inclusive)
    // results are (1, 100), (1, 101), (2, 200)
    auto t1 = create_table({
        "t1",
        {
            { "c0", t::int8(), nullity{false} },
            { "c1", t::int8(), nullity{false} },
        },
    });
    auto primary_idx_t1 = create_primary_index(t1, {0,1}, {});

    auto& take = add_take(2);
    add_column_types(take, t::int8{}, t::int8{});
    auto& target = process_.operators().insert(relation::join_scan{
        relation::join_kind::inner,
        bindings_(*primary_idx_t1),
        {
            {bindings_(t1->columns()[0]), bindings_.stream_variable("c2")},
            {bindings_(t1->columns()[1]), bindings_.stream_variable("c3")},
        },
        relation::join_scan::endpoint{
            {relation::join_scan::key{
                bindings_(t1->columns()[0]),
                varref{take.columns()[0].destination()},
            }},
            relation::endpoint_kind::prefixed_inclusive
        },
        relation::join_scan::endpoint{
            {relation::join_scan::key{
                bindings_(t1->columns()[1]),
                varref{take.columns()[1].destination()},
            }},
            relation::endpoint_kind::prefixed_inclusive
        },
    });

    auto& offer = add_offer(destinations(target.columns()));
    take.output() >> target.left();
    target.output() >> offer.input();

    add_column_types(target, t::int8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int8{});
    create_processor_info();

    auto input = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(1, 2);
    auto output0 = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(1, 100);
    auto output1 = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(1, 101);
    auto output2 = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(2, 200);
    variable_table_info input_variable_info{create_variable_table_info(destinations(take.columns()), input)};
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), output0)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());
    variable_table output_variables{output_variable_info};

    std::vector<jogasaki::mock::basic_record> result{};
    join_scan op{
        relation::join_kind::inner,
        0,
        *processor_info_,
        0,
        *primary_idx_t1,
        target.columns(),
        target.lower().keys(),
        utils::from(target.lower().kind()),
        target.upper().keys(),
        utils::from(target.upper().kind()),
        target.condition(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(output_variables.store().ref(), output0.record_meta()));
        }),
        &input_variable_info,
        &output_variable_info
    };

    put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(1, 100), {});
    put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(2, 200), {});
    put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(1, 101), {});
    put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(3, 300), {});
    auto tx = wrap(db_->create_transaction());
    auto&& [task_ctx, rctx] = create_task_context({}, {}, {}, {}, tx);

    auto match_info = details::match_info_scan{
        op.match_info().begin_fields_,
        op.match_info().begin_endpoint_,
        op.match_info().end_fields_,
        op.match_info().end_endpoint_,
        details::create_secondary_key_fields(nullptr)
    };
    join_scan_context ctx(
        task_ctx.get(),
        input_variables,
        output_variables,
        get_storage(*db_, primary_idx_t1->simple_name()),
        nullptr,
        tx.get(),
        std::make_unique<join_scan_matcher>(
            false,
            match_info,
            op.key_columns(),
            op.value_columns()
        ),
        &resource_,
        &varlen_resource_,
        nullptr
    );

    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ASSERT_EQ(3, result.size());
    EXPECT_EQ(output0, result[0]);
    EXPECT_EQ(output1, result[1]);
    EXPECT_EQ(output2, result[2]);
    ASSERT_EQ(status::ok, tx->commit());
    ctx.release();
}

TEST_F(join_scan_test, secondary_index) {
    // base table t1 (c0 bigint primary key, c1 bigint)
    // join target index i1 on t1 (c1, c0)
    // records:
    //  - (100, 10)
    //  - (200, 20)
    //  - (201, 20)
    //  - (300, 30)
    // join key: i1.c1
    // upstream `take` passes (20, 30) as pair of lower and upper (both bounds are prefixed inclusive)
    // results are (200,20), (201,20), (300,30)
    auto t1 = create_table({
        "t1",
        {
            { "c0", t::int8(), nullity{false} },
            { "c1", t::int8(), nullity{false} },
        },
    });
    auto primary_idx_t1 = create_primary_index(t1, {0}, {1});
    auto secondary_idx_t1 = create_secondary_index(t1, "i1", {1,0}, {});

    auto& take = add_take(2);
    add_column_types(take, t::int8{}, t::int8{});

    auto& target = process_.operators().insert(relation::join_scan{
        relation::join_kind::inner,
        bindings_(*secondary_idx_t1),
        {
            { bindings_(t1->columns()[0]), bindings_.stream_variable("c2") },
            { bindings_(t1->columns()[1]), bindings_.stream_variable("c3")  },
        },
        relation::join_scan::endpoint{
            {relation::join_scan::key{
                bindings_(t1->columns()[0]),
                varref{take.columns()[0].destination()},
            }},
            relation::endpoint_kind::prefixed_inclusive
        },
        relation::join_scan::endpoint{
            {relation::join_scan::key{
                bindings_(t1->columns()[1]),
                varref{take.columns()[1].destination()},
            }},
            relation::endpoint_kind::prefixed_inclusive
        },
    });
    auto& offer = add_offer(destinations(target.columns()));
    take.output() >> target.left();
    target.output() >> offer.input();

    add_column_types(target, t::int8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int8{});
    create_processor_info();

    auto input = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(20, 30);
    auto output0 = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(200, 20);
    auto output1 = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(201, 20);
    auto output2 = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(300, 30);
    variable_table_info input_variable_info{create_variable_table_info(destinations(take.columns()), input)};
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), output0)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());
    variable_table output_variables{output_variable_info};

    std::vector<jogasaki::mock::basic_record> result{};
    join_scan op{
        relation::join_kind::inner,
        0,
        *processor_info_,
        0,
        *primary_idx_t1,
        target.columns(),
        target.lower().keys(),
        utils::from(target.lower().kind()),
        target.upper().keys(),
        utils::from(target.upper().kind()),
        target.condition(),
        secondary_idx_t1.get(),
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(output_variables.store().ref(), output0.record_meta()));
        }),
        &input_variable_info,
        &output_variable_info
    };

    auto k100 = put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8>(100), create_record<kind::int8>(10));
    put_secondary(*db_, secondary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(10, 100), k100);
    auto k200 = put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8>(200), create_record<kind::int8>(20));
    put_secondary(*db_, secondary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(20, 200), k200);
    auto k201 = put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8>(201), create_record<kind::int8>(20));
    put_secondary(*db_, secondary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(20, 201), k201);
    auto k300 = put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8>(300), create_record<kind::int8>(30));
    put_secondary(*db_, secondary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(30, 300), k300);
    auto tx = wrap(db_->create_transaction());
    auto&& [task_ctx, rctx] = create_task_context({}, {}, {}, {}, tx);
    auto match_info = details::match_info_scan{
        op.match_info().begin_fields_,
        op.match_info().begin_endpoint_,
        op.match_info().end_fields_,
        op.match_info().end_endpoint_,
        details::create_secondary_key_fields(secondary_idx_t1.get())
    };
    join_scan_context ctx(
        task_ctx.get(),
        input_variables,
        output_variables,
        get_storage(*db_, primary_idx_t1->simple_name()),
        get_storage(*db_, secondary_idx_t1->simple_name()),
        tx.get(),
        std::make_unique<join_scan_matcher>(
            true,
            match_info,
            op.key_columns(),
            op.value_columns()
        ),
        &resource_,
        &varlen_resource_,
        nullptr
    );

    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(200, 20)), result[0]);
    EXPECT_EQ((jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(201, 20)), result[1]);
    EXPECT_EQ((jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(300, 30)), result[2]);

    ASSERT_EQ(status::ok, tx->commit());
    ctx.release();
}

TEST_F(join_scan_test, host_variable_with_condition_expr) {
    // join target t1 (c0 bigint, c1 bigint, primary key (c0, c1))
    // records:
    //  - (10, 100)
    //  - (10, 101)
    //  - (20, 200)
    // join key: t1.c0
    // upstream `take` passes (10, 10) as pair of lower and upper (both bounds are prefixed inclusive)
    // condition take.lower == hostvariable :p0 (=10)
    // results are (10, 100), (10, 101)
    auto t1 = create_table({
        "t1",
        {
            { "c0", t::int8(), nullity{false} },
            { "c1", t::int8(), nullity{false} },
        },
    });
    auto primary_idx_t1 = create_primary_index(t1, {0, 1}, {});

    auto& take = add_take(2);
    add_column_types(take, t::int8{}, t::int8{});

    auto host_variable_record = jogasaki::mock::create_nullable_record<kind::int8>(10);
    auto p0 = bindings_(register_variable("p0", kind::int8));
    variable_table_info host_variable_info{
        std::unordered_map<variable, std::size_t>{
            {p0, 0},
        },
        std::unordered_map<std::string, takatori::descriptor::variable>{
            {"p0", p0},
        },
        host_variable_record.record_meta()
    };
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    auto& target = process_.operators().insert(relation::join_scan {
        relation::join_kind::inner,
        bindings_(*primary_idx_t1),
        {
            { bindings_(t1->columns()[0]), bindings_.stream_variable("c2") },
            { bindings_(t1->columns()[1]), bindings_.stream_variable("c3")  },
        },
        relation::join_scan::endpoint{
            {relation::join_scan::key{
                bindings_(t1->columns()[0]),
                varref{take.columns()[0].destination()},
            }},
            relation::endpoint_kind::prefixed_inclusive
        },
        relation::join_scan::endpoint{
            {relation::join_scan::key{
                bindings_(t1->columns()[1]),
                varref{take.columns()[1].destination()},
            }},
            relation::endpoint_kind::prefixed_inclusive
        },
        compare {
            comparison_operator::equal,
            varref{take.columns()[0].destination()},
            scalar::variable_reference{p0}
        }
    });

    auto& offer = add_offer(destinations(target.columns()));
    take.output() >> target.left();
    target.output() >> offer.input();

    add_column_types(target, t::int8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int8{});
    expression_map_->bind(*target.condition(), t::boolean{});
    auto& c = static_cast<scalar::compare&>(*target.condition());
    expression_map_->bind(c.left(), t::int8 {});
    expression_map_->bind(c.right(), t::int8 {});
    create_processor_info(&host_variables);

    auto input = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(10, 10);
    auto output = jogasaki::mock::create_nullable_record<kind::int8, kind::int8>();
    variable_table_info input_variable_info{create_variable_table_info(destinations(take.columns()), input)};
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), output)};
    variable_table input_variables{input_variable_info};
    input_variables.store().set(input.ref());
    variable_table output_variables{output_variable_info};

    std::vector<jogasaki::mock::basic_record> result{};
    join_scan op{
        relation::join_kind::inner,
        0,
        *processor_info_,
        0,
        *primary_idx_t1,
        target.columns(),
        target.lower().keys(),
        utils::from(target.lower().kind()),
        target.upper().keys(),
        utils::from(target.upper().kind()),
        target.condition(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(output_variables.store().ref(), output.record_meta()));
        }),
        &input_variable_info,
        &output_variable_info
    };

    put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(10, 100), {});
    put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(20, 200), {});
    put(*db_, primary_idx_t1->simple_name(), create_record<kind::int8, kind::int8>(10, 101), {});
    auto tx = wrap(db_->create_transaction());
    auto&& [task_ctx, rctx] = create_task_context({}, {}, {}, {}, tx);
    auto match_info = details::match_info_scan{
        op.match_info().begin_fields_,
        op.match_info().begin_endpoint_,
        op.match_info().end_fields_,
        op.match_info().end_endpoint_,
        details::create_secondary_key_fields(nullptr)
    };
    join_scan_context ctx(
        task_ctx.get(),
        input_variables,
        output_variables,
        get_storage(*db_, primary_idx_t1->simple_name()),
        nullptr,
        tx.get(),
        std::make_unique<join_scan_matcher>(
            false,
            match_info,
            op.key_columns(),
            op.value_columns()
        ),
        &resource_,
        &varlen_resource_,
        nullptr
    );

    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(10, 100)), result[0]);
    EXPECT_EQ((jogasaki::mock::create_nullable_record<kind::int8, kind::int8>(10, 101)), result[1]);
    ASSERT_EQ(status::ok, tx->commit());
    ctx.release();
}

} // namespace jogasaki::executor::process::impl::ops
