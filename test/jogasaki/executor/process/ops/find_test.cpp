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
#include <jogasaki/executor/process/impl/ops/find.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/type/int.h>
#include <takatori/value/int.h>
#include <takatori/util/object_creator.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/kvs_test_utils.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/executor/process/impl/ops/find_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/coder.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
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
    public kvs_test_utils,
    public operator_test_utils {

public:

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

    auto db = kvs::database::open();
    using kind = meta::field_type_kind;
    put(*db, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    auto stg = db->get_storage(primary_idx->simple_name());
    auto tx = db->create_transaction();
    mock::task_context task_ctx{ {}, {}, {}, {} };
    find_context ctx(&task_ctx, input_variables, output_variables, std::move(stg), nullptr, tx.get(), &resource_, &varlen_resource_);
    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(exp, result[0]);
    ASSERT_EQ(status::ok, tx->commit());
    (void)db->close();
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

    auto db = kvs::database::open();
    using kind = meta::field_type_kind;
    auto p_stg = db->create_storage(primary_idx->simple_name());
    auto s_stg = db->create_storage(secondary_idx->simple_name());

    put( *db, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(100, 10), {});
    put( *db, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    put( *db, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(200, 20), {});
    put( *db, primary_idx->simple_name(), create_record<kind::int4>(21), create_record<kind::float8, kind::int8>(2.1, 200));
    put( *db, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(200, 21), {});

    auto tx = db->create_transaction();
    mock::task_context task_ctx{{}, {}, {}, {}};
    find_context ctx(&task_ctx, input_variables, output_variables, std::move(p_stg), std::move(s_stg), tx.get(), &resource_, &varlen_resource_);

    ASSERT_TRUE(static_cast<bool>(op(ctx)));
    ctx.release();

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    auto exp0 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    auto exp1 = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(21, 2.1, 200);
    EXPECT_EQ(exp0, result[0]);
    EXPECT_EQ(exp1, result[1]);
    ASSERT_EQ(status::ok, tx->commit());
    (void)db->close();
}

}

