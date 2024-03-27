/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/ops/scan.h>
#include <jogasaki/executor/process/impl/ops/scan_context.h>
#include <jogasaki/executor/process/impl/scan_info.h>
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
    auto sinfo = std::make_shared<impl::scan_info>();
    mock::task_context task_ctx{ {}, {}, {}, {sinfo}};
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(), sinfo.get(), &resource_, &varlen_resource_);

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
    auto sinfo = std::make_shared<impl::scan_info>();
    mock::task_context task_ctx{ {}, {}, {}, {sinfo}};
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(), sinfo.get(), &resource_, &varlen_resource_);

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

    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &resource_};
    auto sinfo = builder.create_scan_info(target, *primary_idx);
    mock::task_context task_ctx{ {}, {}, {}, {sinfo}};

    put( *db_, primary_idx->simple_name(), create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/B"}), create_record<kind::float8>(1.0));
    put( *db_, primary_idx->simple_name(), create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/C"}), create_record<kind::float8>(2.0));
    put( *db_, primary_idx->simple_name(), create_record<kind::int8, kind::character>(100, accessor::text{"123456789012345678901234567890/D"}), create_record<kind::float8>(3.0));

    auto tx = wrap(db_->create_transaction());
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(), sinfo.get(), &resource_, &varlen_resource_);

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

    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &resource_};
    auto sinfo = builder.create_scan_info(target, *secondary_idx);
    mock::task_context task_ctx{ {}, {}, {}, {sinfo}};

    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(100, 10), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(200, 20), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(21), create_record<kind::float8, kind::int8>(2.1, 201));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(201, 21), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(30), create_record<kind::float8, kind::int8>(3.0, 300));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(300, 30), {});

    auto tx = wrap(db_->create_transaction());
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), get_storage(*db_, secondary_idx->simple_name()), tx.get(), sinfo.get(), &resource_, &varlen_resource_);

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

    jogasaki::plan::compiler_context compiler_ctx{};
    io_exchange_map exchange_map{};
    operator_builder builder{processor_info_, {}, {}, exchange_map, &resource_};
    auto sinfo = builder.create_scan_info(target, *primary_idx);
    mock::task_context task_ctx{ {}, {}, {}, {sinfo}};

    put( *db_, primary_idx->simple_name(), create_record<kind::int4, kind::int8>(100, 10), create_record<kind::int8>(1));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4, kind::int8>(100, 20), create_record<kind::int8>(2));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4, kind::int8>(100, 30), create_record<kind::int8>(3));

    auto tx = wrap(db_->create_transaction());
    scan_context ctx(&task_ctx, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(), sinfo.get(), &resource_, &varlen_resource_);

    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((jogasaki::mock::create_nullable_record<kind::int4, kind::int8, kind::int8>(100, 20, 2)), result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

}

