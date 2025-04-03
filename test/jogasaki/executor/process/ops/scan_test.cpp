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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <tuple>
#include <unordered_map>
#include <boost/container/container_fwd.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/process.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/endpoint_kind.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/type/character.h>
#include <takatori/type/primitive.h>
#include <takatori/type/varying.h>
#include <takatori/util/exception.h>
#include <takatori/value/character.h>
#include <takatori/value/primitive.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/kvsservice/transaction_option.h>
#include <jogasaki/api/kvsservice/transaction_type.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/abstract/range.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/ops/scan.h>
#include <jogasaki/executor/process/impl/ops/scan_context.h>
#include <jogasaki/executor/process/impl/scan_range.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>

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
using take = relation::step::take_flat;
using buffer = relation::buffer;

namespace storage = yugawara::storage;

using yugawara::variable::nullity;
using yugawara::variable::criteria;

class scan_test :
    public test_root,
    public kvs_test_base,
    public operator_test_utils {

public:

    void SetUp() override {
        kvs_db_setup();
    }
    void TearDown() override {
        kvs_db_teardown();
    }
};

TEST_F(scan_test, simple) {
    auto t0 = create_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false}  },
            { "C2", t::int8(), nullity{false}  },
        },
    });

    auto primary_idx = create_primary_index(t0, {0}, {1,2});

    auto& target = process_.operators().insert(relation::scan {
        bindings_(*primary_idx),
        {
            { bindings_(t0->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t0->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t0->columns()[2]), bindings_.stream_variable("c2") },
        },
    });

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int4{}, t::float8{}, t::int8{});
    create_processor_info();

    auto out = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>();
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};
    scan op{
        0,
        *processor_info_,
        0,
        *primary_idx,
        target.columns(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(output_variables.store().ref(), out.record_meta()));
        }),
        &input_variable_info,
        &output_variable_info
    };

    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));

    auto tx = wrap(db_->create_transaction());
    auto transaction_ctx = std::make_shared<transaction_context>();
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto range = (builder.create_scan_ranges(target))[0];
    mock::task_context task_ctx{ {}, {}, {},{range}};
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(),range.get(), request_context_.request_resource(), &varlen_resource_);
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    auto exp0 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 1.0, 100);
    auto exp1 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    EXPECT_EQ(exp0, result[0]);
    EXPECT_EQ(exp1, result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, nullable_fields) {
    auto t0 = create_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{true}  },
            { "C2", t::int8(), nullity{true}  },
        },
    });
    auto primary_idx = create_primary_index(t0, {0}, {1,2});

    auto& target = process_.operators().insert(relation::scan {
        bindings_(*primary_idx),
        {
            { bindings_(t0->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t0->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t0->columns()[2]), bindings_.stream_variable("c2") },
        },
    });

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int4{}, t::float8{}, t::int8{});
    create_processor_info();

    auto out = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>();
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};
    scan op{
        0,
        *processor_info_,
        0,
        *primary_idx,
        target.columns(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(output_variables.store().ref(), out.record_meta(), &verifier_varlen_resource_);
        }),
        &input_variable_info,
        &output_variable_info
    };

    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(10), create_nullable_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(20), create_nullable_record<kind::float8, kind::int8>(std::forward_as_tuple(0.0, 0), {true, true}));

    auto tx = wrap(db_->create_transaction());
    auto transaction_ctx = std::make_shared<transaction_context>();
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto range = (builder.create_scan_ranges(target))[0];
    mock::task_context task_ctx{ {}, {}, {},{range}};
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(),range.get(), request_context_.request_resource(), &varlen_resource_);
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    auto exp0 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(10, 1.0, 100);
    auto exp1 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(std::forward_as_tuple(20, 0.0, 000), {false, true, true});
    EXPECT_EQ(exp0, result[0]);
    EXPECT_EQ(exp1, result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, scan_info) {
    auto t1 = create_table({
        "T1",
        {
            { "C0", t::int8(), nullity{false} },
            { "C1", t::character(t::varying, 100), nullity{false}  },
            { "C2", t::float8(), nullity{false}  },
        },
    });
    auto primary_idx = create_primary_index(t1, {0,1}, {2});

    using key = relation::scan::key;
    auto& target = process_.operators().insert(relation::scan {
        bindings_(*primary_idx),
        {
            { bindings_(t1->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t1->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t1->columns()[2]), bindings_.stream_variable("c2") },
        },
        {
            {
                key {
                    bindings_(t1->columns()[0]),
                    scalar::immediate { takatori::value::int8(100), takatori::type::int8() }
                },
                key {
                    bindings_(t1->columns()[1]),
                    scalar::immediate { takatori::value::character("123456789012345678901234567890/B"), takatori::type::character(t::varying, 100) }
                },
            },
            relation::endpoint_kind::inclusive,
        },
        {
            {
                key {
                    bindings_(t1->columns()[0]),
                    scalar::immediate { takatori::value::int8(100), takatori::type::int8() }
                },
                key {
                    bindings_(t1->columns()[1]),
                    scalar::immediate { takatori::value::character("123456789012345678901234567890/D"), takatori::type::character(t::varying, 100) }
                },
            },
            relation::endpoint_kind::exclusive,
        }
    });

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int8{}, t::character{t::varying, 100}, t::float8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int8{});
    expression_map_->bind(target.lower().keys()[1].value(), t::character{t::varying, 100});
    expression_map_->bind(target.upper().keys()[0].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[1].value(), t::character{t::varying, 100});
    create_processor_info();

    auto out = jogasaki::mock::create_nullable_record<kind::int8, kind::character, kind::float8>();
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};

    scan op{
        0,
        *processor_info_,
        0,
        *primary_idx,
        target.columns(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(output_variables.store().ref(), out.record_meta(), &verifier_varlen_resource_);
        }),
        &input_variable_info,
        &output_variable_info
    };

    auto transaction_ctx = std::make_shared<transaction_context>();
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto range = (builder.create_scan_ranges(target))[0];
    mock::task_context task_ctx{ {}, {}, {},{range}};

    put( *db_, primary_idx->simple_name(), create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/B"}), create_record<kind::float8>(1.0));
    put( *db_, primary_idx->simple_name(), create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/C"}), create_record<kind::float8>(2.0));
    put( *db_, primary_idx->simple_name(), create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/D"}), create_record<kind::float8>(3.0));

    auto tx = wrap(db_->create_transaction());
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(),range.get(), request_context_.request_resource(), &varlen_resource_);
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(2, result.size());
    auto exp0 = jogasaki::mock::create_nullable_record<kind::int8, kind::character, kind::float8>(100, accessor::text("123456789012345678901234567890/B"), 1.0);
    auto exp1 = jogasaki::mock::create_nullable_record<kind::int8, kind::character, kind::float8>(100, accessor::text("123456789012345678901234567890/C"), 2.0);
    EXPECT_EQ(exp0, result[0]);
    EXPECT_EQ(exp1, result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, secondary_index) {
    auto t0 = create_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false}  },
            { "C2", t::int8(), nullity{false}  },
        },
    });

    auto primary_idx = create_primary_index(t0, {0}, {1,2});
    auto secondary_idx = create_secondary_index(t0, "I1", {2}, {});

    auto& target = process_.operators().insert(relation::scan {
        bindings_(*secondary_idx),
        {
            { bindings_(t0->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t0->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t0->columns()[2]), bindings_.stream_variable("c2") },
        },
        {
            {
                relation::scan::key {
                    bindings_(t0->columns()[2]),
                    scalar::immediate { takatori::value::int8(100), takatori::type::int8() }
                },
            },
            relation::endpoint_kind::exclusive,
        },
        {
            {
                relation::scan::key {
                    bindings_(t0->columns()[2]),
                    scalar::immediate { takatori::value::int8(300), takatori::type::int8() }
                },
            },
            relation::endpoint_kind::exclusive,
        }
    });

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int4{}, t::float8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int8{});
    create_processor_info();

    auto out = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>();
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};
    scan op{
        0,
        *processor_info_,
        0,
        *primary_idx,
        target.columns(),
        secondary_idx.get(),
        std::make_unique<verifier>([&]() {
            result.emplace_back(output_variables.store().ref(), out.record_meta(), &verifier_varlen_resource_);
        }),
        &input_variable_info,
        &output_variable_info
    };
    auto transaction_ctx = std::make_shared<transaction_context>();
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto range = (builder.create_scan_ranges(target))[0];
    mock::task_context task_ctx{ {}, {}, {} ,{range}};

    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(100, 10), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(200, 20), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(21), create_record<kind::float8, kind::int8>(2.1, 201));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(201, 21), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(30), create_record<kind::float8, kind::int8>(3.0, 300));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(300, 30), {});

    auto tx = wrap(db_->create_transaction());
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), get_storage(*db_, secondary_idx->simple_name()), tx.get(),range.get(), request_context_.request_resource(), &varlen_resource_);
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    auto exp0 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    auto exp1= jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(21, 2.1, 201);
    EXPECT_EQ(exp0, result[0]);
    EXPECT_EQ(exp1, result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(scan_test, host_variables) {
    auto t0 = create_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::int8(), nullity{false}  },
            { "C2", t::int8(), nullity{false}  },
        },
    });
    auto primary_idx = create_primary_index(t0, {0,1}, {2});

    auto host_variable_record = jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(100, 10, 100, 30);
    auto p0 = bindings_(register_variable("p0", kind::int4));
    auto p1 = bindings_(register_variable("p1", kind::int8));
    auto p2 = bindings_(register_variable("p2", kind::int4));
    auto p3 = bindings_(register_variable("p3", kind::int8));
    variable_table_info host_variable_info{
        std::unordered_map<variable, std::size_t>{
            {p0, 0},
            {p1, 1},
            {p2, 2},
            {p3, 3},
        },
        std::unordered_map<std::string, takatori::descriptor::variable>{
            {"p0", p0},
            {"p1", p1},
            {"p2", p2},
            {"p3", p3},
        },
        host_variable_record.record_meta()
    };
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    using key = relation::scan::key;
    auto& target = process_.operators().insert(relation::scan {
        bindings_(*primary_idx),
        {
            { bindings_(t0->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t0->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t0->columns()[2]), bindings_.stream_variable("c2") },
        },
        {
            {
                key {
                    bindings_(t0->columns()[0]),
                    scalar::variable_reference{ p0 }
                },
                key {
                    bindings_(t0->columns()[1]),
                    scalar::variable_reference{ p1 }
                },
            },
            relation::endpoint_kind::exclusive,
        },
        {
            {
                key {
                    bindings_(t0->columns()[0]),
                    scalar::variable_reference{ p2 }
                },
                key {
                    bindings_(t0->columns()[1]),
                    scalar::variable_reference{ p3 }
                },
            },
            relation::endpoint_kind::exclusive,
        }
    });

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int4{}, t::int8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int4{});
    expression_map_->bind(target.lower().keys()[1].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int4{});
    expression_map_->bind(target.upper().keys()[1].value(), t::int8{});
    create_processor_info(&host_variables);

    auto out = jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int8>();
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};

    scan op{
        0,
        *processor_info_,
        0,
        *primary_idx,
        target.columns(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(output_variables.store().ref(), out.record_meta(), &verifier_varlen_resource_);
        }),
        &input_variable_info,
        &output_variable_info
    };
    auto transaction_ctx = std::make_shared<transaction_context>();
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto range = (builder.create_scan_ranges(target))[0];
    mock::task_context task_ctx{ {}, {}, {},{range}};

    put( *db_, primary_idx->simple_name(), create_record<kind::int4, kind::int8>(100, 10), create_record<kind::int8>(1));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4, kind::int8>(100, 20), create_record<kind::int8>(2));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4, kind::int8>(100, 30), create_record<kind::int8>(3));

    auto tx = wrap(db_->create_transaction());
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(), range.get(), request_context_.request_resource(), &varlen_resource_);

    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int8>(100, 20, 2)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}
/**
 * @brief Scan information test for #1180 (RTX scan with parallelism of 1)
 *
 * This test is a **scan information validation test related to #1180**, and
 * `scan_test::scan_info_rtx_parallel_1` ensures that the **RTX scan with parallelism of 1**
 * functions correctly.
 *
 * - Ensures that both the **start and end endpoints of the range scan are `exclusive`**
 */
TEST_F(scan_test, scan_info_rtx_parallel_1) {
    // issues #1180
    const int parallel = 1;
    auto cfg           = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->key_distribution(key_distribution_kind::simple);
    global::config_pool(cfg);
    auto t0          = create_table({
                 "T0",
                 {
                     {"C0", t::int4(), nullity{false}},
                     {"C1", t::int8(), nullity{false}},
                     {"C2", t::int8(), nullity{false}},
        },
    });
    auto primary_idx = create_primary_index(t0, {0, 1}, {2});

    auto host_variable_record =
        jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(
            100, 10, 100, 30);
    auto p0 = bindings_(register_variable("p0", kind::int4));
    auto p1 = bindings_(register_variable("p1", kind::int8));
    auto p2 = bindings_(register_variable("p2", kind::int4));
    auto p3 = bindings_(register_variable("p3", kind::int8));
    variable_table_info host_variable_info{std::unordered_map<variable, std::size_t>{
                                               {p0, 0},
                                               {p1, 1},
                                               {p2, 2},
                                               {p3, 3},
                                           },
        std::unordered_map<std::string, takatori::descriptor::variable>{
            {"p0", p0},
            {"p1", p1},
            {"p2", p2},
            {"p3", p3},
        },
        host_variable_record.record_meta()};
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    using key    = relation::scan::key;
    auto first   = relation::endpoint_kind::exclusive;
    auto end     = relation::endpoint_kind::exclusive;
    auto& target = process_.operators().insert(relation::scan{bindings_(*primary_idx),
        {
            {bindings_(t0->columns()[0]), bindings_.stream_variable("c0")},
            {bindings_(t0->columns()[1]), bindings_.stream_variable("c1")},
            {bindings_(t0->columns()[2]), bindings_.stream_variable("c2")},
        },
        {
            {
                key{bindings_(t0->columns()[0]),
                    scalar::immediate{takatori::value::int8(100), takatori::type::int8()}},
                key{bindings_(t0->columns()[1]),
                    scalar::immediate{takatori::value::int8(200), takatori::type::int8()}},
            },
            first,
        },
        {
            {
                key{bindings_(t0->columns()[0]),
                    scalar::immediate{takatori::value::int8(251658240), takatori::type::int8()}},
                key{bindings_(t0->columns()[1]),
                    scalar::immediate{takatori::value::int8(INT64_MIN), takatori::type::int8()}},
            },
            relation::endpoint_kind::exclusive,
        }});

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int4{}, t::int8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int4{});
    expression_map_->bind(target.lower().keys()[1].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int4{});
    expression_map_->bind(target.upper().keys()[1].value(), t::int8{});
    create_processor_info(&host_variables);

    auto out = jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int8>();
    variable_table_info output_variable_info{
        create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};

    scan op{0, *processor_info_, 0, *primary_idx, target.columns(), nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(
                output_variables.store().ref(), out.record_meta(), &verifier_varlen_resource_);
        }),
        &input_variable_info, &output_variable_info};
    std::shared_ptr<kvs::transaction> tra;
    jogasaki::api::kvsservice::table_areas wp{};
    std::vector<std::string> write_preserves      = {"table1", "table2"};
    std::vector<std::string> read_areas_inclusive = {"area1", "area2"};
    std::vector<std::string> read_areas_exclusive = {"area3", "area4"};
    kvs::transaction_option opt(kvs::transaction_option::transaction_type::read_only,
        write_preserves, read_areas_inclusive, read_areas_exclusive);
    auto opt_ptr         = std::make_shared<jogasaki::kvs::transaction_option>(opt);
    auto transaction_ctx = std::make_shared<transaction_context>(tra, opt_ptr);
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto zzz = builder.create_scan_ranges(target);
    ASSERT_EQ(parallel, zzz.size());
    EXPECT_EQ(zzz[0]->begin().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
    EXPECT_EQ(zzz[0]->end().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
}
/**
 * @brief Scan information test for #1180 (RTX scan with parallelism of 2)
 *
 * This test is a **scan information validation test related to #1180**, and
 * `scan_test::scan_info_rtx_parallel_2` ensures that the **RTX scan with parallelism of 2**
 * functions correctly.
 *
 * - Ensures that the **start endpoint is `exclusive`** for the first range and the **end endpoint is `inclusive`** for the second range.
 */
TEST_F(scan_test, scan_info_rtx_parallel_2) {
    // issues #1180
    const int parallel = 2;
    auto cfg           = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->key_distribution(key_distribution_kind::simple);
    global::config_pool(cfg);
    auto t0          = create_table({
                 "T0",
                 {
                     {"C0", t::int4(), nullity{false}},
                     {"C1", t::int8(), nullity{false}},
                     {"C2", t::int8(), nullity{false}},
        },
    });
    auto primary_idx = create_primary_index(t0, {0, 1}, {2});

    auto host_variable_record =
        jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(
            100, 10, 100, 30);
    auto p0 = bindings_(register_variable("p0", kind::int4));
    auto p1 = bindings_(register_variable("p1", kind::int8));
    auto p2 = bindings_(register_variable("p2", kind::int4));
    auto p3 = bindings_(register_variable("p3", kind::int8));
    variable_table_info host_variable_info{std::unordered_map<variable, std::size_t>{
                                               {p0, 0},
                                               {p1, 1},
                                               {p2, 2},
                                               {p3, 3},
                                           },
        std::unordered_map<std::string, takatori::descriptor::variable>{
            {"p0", p0},
            {"p1", p1},
            {"p2", p2},
            {"p3", p3},
        },
        host_variable_record.record_meta()};
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    using key    = relation::scan::key;
    auto first   = relation::endpoint_kind::exclusive;
    auto end     = relation::endpoint_kind::exclusive;
    auto& target = process_.operators().insert(relation::scan{bindings_(*primary_idx),
        {
            {bindings_(t0->columns()[0]), bindings_.stream_variable("c0")},
            {bindings_(t0->columns()[1]), bindings_.stream_variable("c1")},
            {bindings_(t0->columns()[2]), bindings_.stream_variable("c2")},
        },
        {
            {
                key{
                    bindings_(t0->columns()[0]),
                    scalar::immediate{takatori::value::int8(100), takatori::type::int8()}
                    /*scalar::variable_reference{ p0 }*/
                },
                key{
                    bindings_(t0->columns()[1]),
                    scalar::immediate{takatori::value::int8(200), takatori::type::int8()}
                    /*scalar::variable_reference{ p1 }*/
                },
            },
            first,
        },
        {
            {
                key{bindings_(t0->columns()[0]),
                    scalar::immediate{takatori::value::int8(251658240), takatori::type::int8()}},
                key{bindings_(t0->columns()[1]),
                    scalar::immediate{takatori::value::int8(INT64_MIN), takatori::type::int8()}},
            },
            relation::endpoint_kind::exclusive,
        }});

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int4{}, t::int8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int4{});
    expression_map_->bind(target.lower().keys()[1].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int4{});
    expression_map_->bind(target.upper().keys()[1].value(), t::int8{});
    create_processor_info(&host_variables);

    auto out = jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int8>();
    variable_table_info output_variable_info{
        create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};

    scan op{0, *processor_info_, 0, *primary_idx, target.columns(), nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(
                output_variables.store().ref(), out.record_meta(), &verifier_varlen_resource_);
        }),
        &input_variable_info, &output_variable_info};
    std::shared_ptr<kvs::transaction> tra;
    jogasaki::api::kvsservice::table_areas wp{};
    std::vector<std::string> write_preserves      = {"table1", "table2"};
    std::vector<std::string> read_areas_inclusive = {"area1", "area2"};
    std::vector<std::string> read_areas_exclusive = {"area3", "area4"};
    kvs::transaction_option opt(kvs::transaction_option::transaction_type::read_only,
        write_preserves, read_areas_inclusive, read_areas_exclusive);
    auto opt_ptr         = std::make_shared<jogasaki::kvs::transaction_option>(opt);
    auto transaction_ctx = std::make_shared<transaction_context>(tra, opt_ptr);
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto zzz = builder.create_scan_ranges(target);
    ASSERT_EQ(parallel, zzz.size());
    EXPECT_EQ(zzz[0]->begin().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
    EXPECT_EQ(zzz[0]->end().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
    EXPECT_EQ(zzz[1]->begin().endpointkind(), jogasaki::kvs::end_point_kind::inclusive);
    EXPECT_EQ(zzz[1]->end().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
}
/**
 * @brief #1180 Scan Information Test (RTX Scan with Parallelism 4)
 *
 * This test is related to **issue #1180**, which verifies the correct functionality of
 * the **RTX scan with parallelism 4**. The test ensures that scan ranges are properly
 * divided into 4 parts when `scan_default_parallel(4)` is used.
 *
 * - Checks that the `begin` and `end` endpoints of the ranges are correctly assigned:
 *   - The first range begins with an **exclusive** endpoint, while the following ranges
 *     have an **inclusive** start and **exclusive** end.
 * - Verifies the accuracy of endpoint kinds in the created scan ranges.
 */
TEST_F(scan_test, scan_info_rtx_parallel_4) {
    // issues #1180
    const int parallel = 4;
    auto cfg           = std::make_shared<configuration>();
    cfg->rtx_parallel_scan(true);
    cfg->scan_default_parallel(parallel);
    cfg->key_distribution(key_distribution_kind::simple);
    global::config_pool(cfg);
    auto t0          = create_table({
                 "T0",
                 {
                     {"C0", t::int4(), nullity{false}},
                     {"C1", t::int8(), nullity{false}},
                     {"C2", t::int8(), nullity{false}},
        },
    });
    auto primary_idx = create_primary_index(t0, {0, 1}, {2});

    auto host_variable_record =
        jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(
            100, 10, 100, 30);
    auto p0 = bindings_(register_variable("p0", kind::int4));
    auto p1 = bindings_(register_variable("p1", kind::int8));
    auto p2 = bindings_(register_variable("p2", kind::int4));
    auto p3 = bindings_(register_variable("p3", kind::int8));
    variable_table_info host_variable_info{std::unordered_map<variable, std::size_t>{
                                               {p0, 0},
                                               {p1, 1},
                                               {p2, 2},
                                               {p3, 3},
                                           },
        std::unordered_map<std::string, takatori::descriptor::variable>{
            {"p0", p0},
            {"p1", p1},
            {"p2", p2},
            {"p3", p3},
        },
        host_variable_record.record_meta()};
    variable_table host_variables{host_variable_info};
    host_variables.store().set(host_variable_record.ref());

    using key    = relation::scan::key;
    auto first   = relation::endpoint_kind::exclusive;
    auto end     = relation::endpoint_kind::exclusive;
    auto& target = process_.operators().insert(relation::scan{bindings_(*primary_idx),
        {
            {bindings_(t0->columns()[0]), bindings_.stream_variable("c0")},
            {bindings_(t0->columns()[1]), bindings_.stream_variable("c1")},
            {bindings_(t0->columns()[2]), bindings_.stream_variable("c2")},
        },
        {
            {
                key{bindings_(t0->columns()[0]),
                    scalar::immediate{takatori::value::int8(100), takatori::type::int8()}},
                key{bindings_(t0->columns()[1]),
                    scalar::immediate{takatori::value::int8(200), takatori::type::int8()}},
            },
            first,
        },
        {
            {
                key{bindings_(t0->columns()[0]),
                    scalar::immediate{takatori::value::int8(251658240), takatori::type::int8()}},
                key{bindings_(t0->columns()[1]),
                    scalar::immediate{takatori::value::int8(INT64_MIN), takatori::type::int8()}},
            },
            relation::endpoint_kind::exclusive,
        }});

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();

    add_column_types(target, t::int4{}, t::int8{}, t::int8{});
    expression_map_->bind(target.lower().keys()[0].value(), t::int4{});
    expression_map_->bind(target.lower().keys()[1].value(), t::int8{});
    expression_map_->bind(target.upper().keys()[0].value(), t::int4{});
    expression_map_->bind(target.upper().keys()[1].value(), t::int8{});
    create_processor_info(&host_variables);

    auto out = jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int8>();
    variable_table_info output_variable_info{
        create_variable_table_info(destinations(target.columns()), out)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};

    scan op{0, *processor_info_, 0, *primary_idx, target.columns(), nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(
                output_variables.store().ref(), out.record_meta(), &verifier_varlen_resource_);
        }),
        &input_variable_info, &output_variable_info};
    std::shared_ptr<kvs::transaction> tra;
    jogasaki::api::kvsservice::table_areas wp{};
    std::vector<std::string> write_preserves      = {"table1", "table2"};
    std::vector<std::string> read_areas_inclusive = {"area1", "area2"};
    std::vector<std::string> read_areas_exclusive = {"area3", "area4"};
    kvs::transaction_option opt(kvs::transaction_option::transaction_type::read_only,
        write_preserves, read_areas_inclusive, read_areas_exclusive);
    auto opt_ptr         = std::make_shared<jogasaki::kvs::transaction_option>(opt);
    auto transaction_ctx = std::make_shared<transaction_context>(tra, opt_ptr);
    transaction_ctx->error_info(create_error_info(error_code::none, "", status::err_unknown));
    request_context_.transaction(transaction_ctx);
    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
    auto zzz = builder.create_scan_ranges(target);
    ASSERT_EQ(parallel, zzz.size());
    EXPECT_EQ(zzz[0]->begin().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
    EXPECT_EQ(zzz[0]->end().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
    EXPECT_EQ(zzz[1]->begin().endpointkind(), jogasaki::kvs::end_point_kind::inclusive);
    EXPECT_EQ(zzz[1]->end().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
    EXPECT_EQ(zzz[2]->begin().endpointkind(), jogasaki::kvs::end_point_kind::inclusive);
    EXPECT_EQ(zzz[2]->end().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
    EXPECT_EQ(zzz[3]->begin().endpointkind(), jogasaki::kvs::end_point_kind::inclusive);
    EXPECT_EQ(zzz[3]->end().endpointkind(), jogasaki::kvs::end_point_kind::exclusive);
}
} // namespace jogasaki::executor::process::impl::ops
