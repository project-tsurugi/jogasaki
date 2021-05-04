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
    void add_types(Args&&... types) {
        std::vector<std::reference_wrapper<takatori::type::data>> v{types...};
        std::size_t i=0;
        for(auto&& type : v) {
            yugawara::analyzer::variable_resolution r{std::move(static_cast<takatori::type::data&>(type))};
            variable_map_->bind(target_->columns()[i].source(), r);
            variable_map_->bind(target_->columns()[i].destination(), r);
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

    target_ = &process_.operators().insert(relation::find {
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

    add_downstream(3, target_->columns());
    add_types(t::int4{}, t::float8{}, t::int8{});
    expression_map_->bind(target_->keys()[0].value(), t::int4{});
    create_processor_info();
//    variable_table variables{processor_info_->vars_info_list()[0]};
    auto exp = jogasaki::mock::create_nullable_record<kind::int4, kind::float8, kind::int8>(20, 2.0, 200);
    auto tmeta = exp.record_meta();
    variable_table_info block_info{
        {
            { target_->columns()[0].destination(), { tmeta->value_offset(0), tmeta->nullity_offset(0), } },
            { target_->columns()[1].destination(), { tmeta->value_offset(1), tmeta->nullity_offset(1), } },
            { target_->columns()[2].destination(), { tmeta->value_offset(2), tmeta->nullity_offset(2), } },
        },
        tmeta,
    };
    variable_table variables{block_info};

    std::vector<jogasaki::mock::basic_record> result{};
    find s{
        0,
        *processor_info_,
        0,
        target_->keys(),
        *primary_idx,
        target_->columns(),
        nullptr,
        std::make_unique<verifier>([&]() {
            result.emplace_back(jogasaki::mock::basic_record(variables.store().ref(), tmeta));
        })
    };

    auto db = kvs::database::open();
    using kind = meta::field_type_kind;
    put(*db, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    auto stg = db->get_storage(primary_idx->simple_name());
    auto tx = db->create_transaction();
    mock::task_context task_ctx{ {}, {}, {}, {} };
    find_context ctx(&task_ctx, variables, std::move(stg), nullptr, tx.get(), &resource_, &varlen_resource_);
    s(ctx);
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
    target_ = &process_.operators().insert(relation::find {
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

    add_downstream(3, target_->columns());
    add_types(t::int4{}, t::float8{}, t::int8{});
    expression_map_->bind(target_->keys()[0].value(), t::int8{});
    create_processor_info();

    using kind = meta::field_type_kind;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    find s{
        0,
        *processor_info_,
        0,
        target_->keys(),
        *primary_idx,
        target_->columns(),
        secondary_idx.get(),
        std::move(d)
    };

    auto& block_info = processor_info_->vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    mock::task_context task_ctx{{}, {}, {}, {}};

    auto db = kvs::database::open();
    auto p_stg = db->create_storage(primary_idx->simple_name());
    auto s_stg = db->create_storage(secondary_idx->simple_name());

    put( *db, primary_idx->simple_name(), create_record<kind::int4>(10), create_record<kind::float8, kind::int8>(1.0, 100));
    put( *db, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(100, 10), {});
    put( *db, primary_idx->simple_name(), create_record<kind::int4>(20), create_record<kind::float8, kind::int8>(2.0, 200));
    put( *db, secondary_idx->simple_name(), create_record<kind::int8, kind::int4>(200, 20), {});

    auto tx = db->create_transaction();
    auto t = tx.get();
    find_context ctx(&task_ctx, variables, std::move(p_stg), std::move(s_stg), t, &resource_, &varlen_resource_);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(target_->columns()[0].destination()).value_offset();
    auto c1_offset = map.at(target_->columns()[1].destination()).value_offset();
    auto c2_offset = map.at(target_->columns()[2].destination()).value_offset();

    std::size_t count = 0;
    downstream->body([&]() {
        switch(count) {
            case 0: {
                EXPECT_EQ(20, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(2.0, vars_ref.get_value<double>(c1_offset));
                EXPECT_EQ(200, vars_ref.get_value<std::int64_t>(c2_offset));
                break;
            }
            default:
                ADD_FAILURE();
        }
        ++count;
    });
    s(ctx);
    ctx.release();
    ASSERT_EQ(1, count);
    ASSERT_EQ(status::ok, t->commit());
    (void)db->close();
}

}

