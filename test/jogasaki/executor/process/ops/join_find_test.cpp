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
#include <jogasaki/executor/process/impl/ops/join_find.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <takatori/plan/forward.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/util/object_creator.h>
#include <takatori/relation/step/take_cogroup.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/join_find.h>
#include <takatori/plan/group.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/kvs_test_utils.h>

#include <jogasaki/meta/variable_order.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/impl/ops/join.h>
#include <jogasaki/executor/process/impl/ops/take_cogroup.h>
#include <jogasaki/executor/process/mock/group_reader.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/executor/process/mock/iterable_group_store.h>

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

class join_find_test : public test_root, public kvs_test_utils {
public:
};

using kind = field_type_kind;
using group_reader = mock::basic_group_reader;
using group_type = group_reader::group_type;
using keys_type = group_type::key_type;
using values_type = group_type::value_type;

using yugawara::variable::nullity;
using yugawara::variable::criteria;

TEST_F(join_find_test, simple) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table({
        "T0",
        {
            { "C0", t::int8(), nullity{false} },
            { "C1", t::int8(), nullity{false} },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];

    std::shared_ptr<::yugawara::storage::index> primary_idx_t0 = storages->add_index({
        t0,
        t0->simple_name(),
        {
            t0->columns()[0],
        },
        {
            t0->columns()[1],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });
    std::shared_ptr<storage::table> t1 = storages->add_table({
        "T1",
        {
            { "C0", t::int8(), nullity{false} },
            { "C1", t::int8(), nullity{false} },
        },
    });
    storage::column const& t1c0 = t1->columns()[0];
    storage::column const& t1c1 = t1->columns()[1];

    std::shared_ptr<::yugawara::storage::index> primary_idx_t1 = storages->add_index({
        t1,
        t1->simple_name(),
        {
            t1->columns()[0],
        },
        {
            t1->columns()[1],
        },
        {
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        },
    });

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process{});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");

    auto& scan = p0.operators().insert(relation::scan {
        bindings(*primary_idx_t0),
        {
            { bindings(t0c0), c0 },
            { bindings(t0c1), c1 },
        },
    });

    auto c2 = bindings.stream_variable("c2");
    auto c3 = bindings.stream_variable("c3");

    auto& r0 = p0.operators().insert(relation::join_find {
        relation::join_kind::inner,
        bindings(*primary_idx_t1),
        {
            { bindings(t1c0), c2 },
            { bindings(t1c1), c3 },
        },
        {
            relation::join_find::key{
                bindings(t1c0),
                varref{c0},
            }
        },
    });

    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };

    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    auto&& f1c3 = f1.columns()[3];
    // without offer, the columns are not used and block variables become empty
    auto&& r1 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
            { c3, f1c3 },
        },
    });

    scan.output() >> r0.left();
    r0.output() >> r1.input(); // connection required by takatori

    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(c0, t::int8{});
    vmap->bind(c1, t::int8{});
    vmap->bind(c2, t::int8{});
    vmap->bind(c3, t::int8{});
    auto emap = std::make_shared<yugawara::analyzer::expression_mapping>();
    emap->bind(r0.keys()[0].value(), t::int8{});
    yugawara::compiled_info c_info{emap, vmap};

    processor_info p_info{p0.operators(), c_info};

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&global::page_pool()};
    auto d = std::make_unique<verifier>();
    auto downstream = d.get();
    join_find s{
        0,
        p_info,
        0,
        primary_idx_t1->simple_name(),
        *primary_idx_t1,
        r0.columns(),
        r0.keys(),
        r0.condition(),
        std::move(d)
    };

    variable_table_info block_info{p_info.vars_info_list()[0]};
    variable_table variables{block_info};
    auto tmeta = block_info.meta();

    mock::task_context task_ctx{
        {},
        {},
        {},
        {},
    };

    auto db = kvs::database::open();
    auto stg = db->create_storage(primary_idx_t1->simple_name());
    put(
        *db,
        primary_idx_t1->simple_name(),
        create_record<kind::int8>(1),
        create_record<kind::int8>(100)
    );
    put(
        *db,
        primary_idx_t1->simple_name(),
        create_record<kind::int8>(2),
        create_record<kind::int8>(200)
    );
    auto tx = db->create_transaction();
    join_find_context ctx(
        &task_ctx,
        variables,
        std::move(stg),
        tx.get(),
        std::make_unique<details::matcher>(
            s.key_fields(),
            s.key_columns(),
            s.value_columns()
        ),
        &resource,
        &varlen_resource
    );

    auto vars_ref = variables.store().ref();
    auto& map = variables.info();
    vars_ref.set_value<std::int64_t>(map.at(c0).value_offset(), 1);
    vars_ref.set_null(map.at(c0).nullity_offset(), false);
    vars_ref.set_value<std::int64_t>(map.at(c1).value_offset(), 10);
    vars_ref.set_null(map.at(c1).nullity_offset(), false);

    std::vector<jogasaki::mock::basic_record> result{};

    downstream->body([&]() {
        result.emplace_back(jogasaki::mock::basic_record(variables.store().ref(), tmeta));
    });

    s(ctx);

    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, vars_ref.get_value<std::int64_t>(map.at(c0).value_offset()));
    EXPECT_EQ(10, vars_ref.get_value<std::int64_t>(map.at(c1).value_offset()));
    EXPECT_EQ(1, vars_ref.get_value<std::int64_t>(map.at(c2).value_offset()));
    EXPECT_EQ(100, vars_ref.get_value<std::int64_t>(map.at(c3).value_offset()));
    ctx.release();
}

}

