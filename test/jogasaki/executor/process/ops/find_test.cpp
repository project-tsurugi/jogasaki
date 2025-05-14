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
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <boost/container/container_fwd.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/process.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/type/data.h>
#include <takatori/type/primitive.h>
#include <takatori/util/exception.h>
#include <takatori/value/primitive.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/analyzer/variable_resolution.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/process/impl/ops/find.h>
#include <jogasaki/executor/process/impl/ops/find_context.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/status.h>
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

using yugawara::variable::nullity;
using yugawara::variable::criteria;
using yugawara::storage::table;
using yugawara::storage::index;
using yugawara::storage::index_feature_set;

class find_test :
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
    template <class ...Args>
    void add_types(relation::find& target, Args&&... types) {
        std::vector<std::reference_wrapper<takatori::type::data>> v{types...};
        std::size_t i=0;
        for(auto&& type : v) {
            yugawara::analyzer::variable_resolution r{std::move(static_cast<takatori::type::data&>(type))};
            variable_map_->bind(target.columns()[i].source(), r);
            variable_map_->bind(target.columns()[i].destination(), r);
            ++i;
        }
    }
};

TEST_F(find_test, simple) {
    auto t0 = create_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false} },
            { "C2", t::int8(), nullity{false} },
        },
    });
    auto primary_idx = create_primary_index(t0, {0}, {1,2});
    auto& target = process_.operators().insert(relation::find {
        bindings_(*primary_idx),
        {
            { bindings_(t0->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t0->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t0->columns()[2]), bindings_.stream_variable("c2") },
        },
        {
            relation::find::key{
                bindings_(t0->columns()[0]),
                scalar::immediate{takatori::value::int4(20), takatori::type::int4()}
            }
        }
    });
    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();
    add_types(target, t::int4{}, t::float8{}, t::int8{});
    expression_map_->bind(target.keys()[0].value(), t::int4{});
    create_processor_info();

    auto exp = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), exp)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};

    std::vector<jogasaki::mock::basic_record> result{};
    find op{
        0,
        *processor_info_,
        0,
        target.keys(),
        *primary_idx,
        target.columns(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(output_variables.store().ref(), exp.record_meta()));
        }),
        &input_variable_info,
        &output_variable_info
    };

    using kind = meta::field_type_kind;
    put(*db_, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    auto tx = wrap(db_->create_transaction());
    mock::task_context task_ctx{ {}, {}, {}, {} };
    find_context ctx(&task_ctx, input_variables, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(), &resource_, &varlen_resource_, nullptr);
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(exp, result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(find_test, secondary_index) {
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
    auto& target = process_.operators().insert(relation::find {
        bindings_(*secondary_idx),
        {
            { bindings_(t0->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t0->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t0->columns()[2]), bindings_.stream_variable("c2") },
        },
        {
            relation::find::key{
                bindings_(t0->columns()[2]),
                scalar::immediate{takatori::value::int8(200), takatori::type::int8()}
            }
        }
    });

    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();
    add_types(target, t::int4{}, t::float8{}, t::int8{});
    expression_map_->bind(target.keys()[0].value(), t::int8{});
    create_processor_info();
    auto exp = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), exp)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};
    std::vector<jogasaki::mock::basic_record> result{};

    find op{
        0,
        *processor_info_,
        0,
        target.keys(),
        *primary_idx,
        target.columns(),
        secondary_idx.get(),
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(output_variables.store().ref(), exp.record_meta()));
        }),
        &input_variable_info,
        &output_variable_info
    };

    using kind = meta::field_type_kind;

    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(100, 10), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(200, 20), {});
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(21), create_record<kind::float8, kind::int8>(2.1, 200));
    put( *db_, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(200, 21), {});

    auto tx = wrap(db_->create_transaction());
    mock::task_context task_ctx{{}, {}, {}, {}};
    find_context ctx(&task_ctx, input_variables, output_variables, get_storage(*db_, primary_idx->simple_name()), get_storage(*db_, secondary_idx->simple_name()), tx.get(), &resource_, &varlen_resource_, nullptr);

    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    auto exp0 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    auto exp1 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(21, 2.1, 200);
    EXPECT_EQ(exp0, result[0]);
    EXPECT_EQ(exp1, result[1]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(find_test, host_variable) {
    auto t0 = create_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false} },
            { "C2", t::int8(), nullity{false} },
        },
    });
    auto primary_idx = create_primary_index(t0, {0}, {1,2});

    auto host_variable_record = jogasaki::mock::create_nullable_record<kind::int4>(20);
    auto p0 = bindings_(register_variable("p0", kind::int4));
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

    auto& target = process_.operators().insert(relation::find {
        bindings_(*primary_idx),
        {
            { bindings_(t0->columns()[0]), bindings_.stream_variable("c0") },
            { bindings_(t0->columns()[1]), bindings_.stream_variable("c1") },
            { bindings_(t0->columns()[2]), bindings_.stream_variable("c2") },
        },
        {
            relation::find::key{
                bindings_(t0->columns()[0]),
                scalar::variable_reference{p0}
            }
        }
    });
    auto& offer = add_offer(destinations(target.columns()));
    target.output() >> offer.input();
    add_types(target, t::int4{}, t::float8{}, t::int8{});
    expression_map_->bind(target.keys()[0].value(), t::int4{});
    create_processor_info(&host_variables);

    auto exp = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    variable_table_info output_variable_info{create_variable_table_info(destinations(target.columns()), exp)};
    variable_table_info input_variable_info{};
    variable_table input_variables{input_variable_info};
    variable_table output_variables{output_variable_info};

    std::vector<jogasaki::mock::basic_record> result{};
    find op{
        0,
        *processor_info_,
        0,
        target.keys(),
        *primary_idx,
        target.columns(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(output_variables.store().ref(), exp.record_meta()));
        }),
        &input_variable_info,
        &output_variable_info
    };

    using kind = meta::field_type_kind;
    put(*db_, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db_, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    auto tx = wrap(db_->create_transaction());
    mock::task_context task_ctx{ {}, {}, {}, {} };
    find_context ctx(&task_ctx, input_variables, output_variables, get_storage(*db_, primary_idx->simple_name()), nullptr, tx.get(), &resource_, &varlen_resource_, nullptr);
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(exp, result[0]);
    ASSERT_EQ(status::ok, tx->commit());
}

}

