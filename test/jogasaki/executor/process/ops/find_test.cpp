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
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/kvs_test_utils.h>
#include <jogasaki/executor/process/impl/ops/find_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/plan/compiler.h>
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

namespace storage = yugawara::storage;

using yugawara::variable::nullity;
using yugawara::variable::criteria;

class find_test : public test_root, public kvs_test_utils {
public:
};

TEST_F(find_test, simple) {

    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false}  },
            { "C2", t::int8(), nullity{false}  },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<::yugawara::storage::index> primary_idx = storages->add_index({
        t0,
        t0->simple_name(),
        {
            t0->columns()[0],
        },
        {
            t0->columns()[1],
            t0->columns()[2],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");

    auto v0 = bindings(t0c0);
    auto v1 = bindings(t0c1);
    auto v2 = bindings(t0c2);
    auto& r0 = p0.operators().insert(relation::find {
        bindings(*primary_idx),
        {
            { v0, c0 },
            { v1, c1 },
            { v2, c2 },
        },
        {
            relation::find::key{
                v0,
                scalar::immediate{takatori::value::int4(20), takatori::type::int4()}
            }
        }
    });


    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    // without offer, the columns are not used and block variables become empty
    auto&& r1 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
        },
    });
    r0.output() >> r1.input(); // connection required by takatori

    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(v0, t::int4{});
    vmap->bind(v1, t::float8{});
    vmap->bind(v2, t::int8{});
    vmap->bind(c0, t::int4{});
    vmap->bind(c1, t::float8{});
    vmap->bind(c2, t::int8{});
    auto emap = std::make_shared<yugawara::analyzer::expression_mapping>();
    emap->bind(r0.keys()[0].value(), t::int4{});
    yugawara::compiled_info c_info{emap, vmap};
    processor_info p_info{p0.operators(), c_info};

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};
    using kind = meta::field_type_kind;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    find s{
        0,
        p_info,
        0,
        r0.keys(),
        *primary_idx,
        r0.columns(),
        nullptr,
        std::move(d)
    };

    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    auto db = kvs::database::open();
    put(
        *db,
        primary_idx->simple_name(),
        create_record<kind::int4>(10),
        create_record<kind::float8, kind::int8>(1.0, 100)
    );
    put(
        *db,
        primary_idx->simple_name(),
        create_record<kind::int4>(20),
        create_record<kind::float8, kind::int8>(2.0, 200)
    );
    auto stg = db->get_storage(primary_idx->simple_name());

    auto tx = db->create_transaction();
    auto t = tx.get();
    find_context ctx(
        &task_ctx, variables, std::move(stg), nullptr, t, &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();

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

TEST_F(find_test, secondary_index) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::int4(), nullity{false} },
            { "C1", t::float8(), nullity{false}  },
            { "C2", t::int8(), nullity{false}  },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<::yugawara::storage::index> primary_idx = storages->add_index({
        t0,
        t0->simple_name(),
        {
            t0->columns()[0],
        },
        {
            t0->columns()[1],
            t0->columns()[2],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });
    std::shared_ptr<::yugawara::storage::index> secondary_idx = storages->add_index({
        t0,
        "I1",
        {
            t0->columns()[2],
        },
        {
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::primary,
        },
    });

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");

    auto v0 = bindings(t0c0);
    auto v1 = bindings(t0c1);
    auto v2 = bindings(t0c2);
    auto& r0 = p0.operators().insert(relation::find {
        bindings(*secondary_idx),
        {
            { v0, c0 },
            { v1, c1 },
            { v2, c2 },
        },
        {
            relation::find::key{
                v2,
                scalar::immediate{takatori::value::int8(200), takatori::type::int8()}
            }
        }
    });

    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    // without offer, the columns are not used and block variables become empty
    auto&& r1 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
        },
    });
    r0.output() >> r1.input(); // connection required by takatori

    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(v0, t::int4{});
    vmap->bind(v1, t::float8{});
    vmap->bind(v2, t::int8{});
    vmap->bind(c0, t::int4{});
    vmap->bind(c1, t::float8{});
    vmap->bind(c2, t::int8{});
    auto emap = std::make_shared<yugawara::analyzer::expression_mapping>();
    emap->bind(r0.keys()[0].value(), t::int8{});
    yugawara::compiled_info c_info{emap, vmap};

    processor_info p_info{p0.operators(), c_info};

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};
    using kind = meta::field_type_kind;
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    find s{
        0,
        p_info,
        0,
        r0.keys(),
        *primary_idx,
        r0.columns(),
        secondary_idx.get(),
        std::move(d)
    };

    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    auto db = kvs::database::open();
    auto p_stg = db->create_storage(primary_idx->simple_name());
    auto s_stg = db->create_storage(secondary_idx->simple_name());

    put(
        *db,
        primary_idx->simple_name(),
        create_record<kind::int4>(10),
        create_record<kind::float8, kind::int8>(1.0, 100)
    );
    put(
        *db,
        secondary_idx->simple_name(),
        create_record<kind::int8, kind::int4>(100, 10),
        {}
    );
    put(
        *db,
        primary_idx->simple_name(),
        create_record<kind::int4>(20),
        create_record<kind::float8, kind::int8>(2.0, 200)
    );
    put(
        *db,
        secondary_idx->simple_name(),
        create_record<kind::int8, kind::int4>(200, 20),
        {}
    );

    auto tx = db->create_transaction();
    auto t = tx.get();
    find_context ctx(
        &task_ctx, variables, std::move(p_stg), std::move(s_stg), t, &resource, &varlen_resource);

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();

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

