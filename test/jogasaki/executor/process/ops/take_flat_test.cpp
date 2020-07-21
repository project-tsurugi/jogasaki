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
#include <jogasaki/executor/process/impl/ops/take_flat.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/take_flat_context.h>

#include <jogasaki/basic_record.h>
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
using take = relation::step::take_flat;
using buffer = relation::buffer;

namespace storage = yugawara::storage;

class take_flat_test : public test_root {};

TEST_F(take_flat_test, simple) {
    binding::factory bindings;

    ::takatori::plan::forward f0 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f0c0 = f0.columns()[0];
    auto&& f0c1 = f0.columns()[1];
    auto&& f0c2 = f0.columns()[2];

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");
    auto& r0 = p0.operators().insert(relation::step::take_flat {
        bindings(f0),
        {
            { f0c0, c0 },
            { f0c1, c1 },
            { f0c2, c2 },
        },
    });

    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f0.columns()[0];
    auto&& f1c1 = f0.columns()[1];
    auto&& f1c2 = f0.columns()[2];
    // without offer, the columns are not used and block variables become empty
    auto&& r1 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f0),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
        },
    });
    r0.output() >> r1.input(); // connection required by takatori

    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(f0c0, t::int4{});
    vmap->bind(f0c1, t::int4{});
    vmap->bind(f0c2, t::int4{});
    vmap->bind(c0, t::int4{});
    vmap->bind(c1, t::int4{});
    vmap->bind(c2, t::int4{});
    yugawara::compiled_info c_info{{}, vmap};

    processor_info p_info{p0.operators(), c_info};

    // currently this vector order defines the order of variables
    // TODO fix when the logic is fixed
    std::vector<variable, takatori::util::object_allocator<variable>> columns{f0c1, f0c0, f0c2};
    variable_order order{
        variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
        columns
    };

    take_flat s{
        p_info, 0, order, {
            {f0c0, c0},
            {f0c1, c1},
            {f0c2, c2},
        },
        0
    };

    auto& block_info = p_info.blocks_info()[s.block_index()];
    block_variables variables{block_info};

    using test_record = basic_record<kind::int4, kind::int4, kind::int4>;
    std::vector<test_record> records{
        test_record{0, 1, 2},
        test_record{0, 2, 4},
    };
    auto reader = std::make_shared<mock::basic_record_reader<test_record>>(records);

    mock::task_context task_ctx{
        {reader_container{reader.get()}},
        {},
        {},
        {},
    };

    take_flat_context ctx(&task_ctx, variables);

    auto block_rec = variables.store().ref();
    auto map = variables.value_map();
    auto block_rec_meta = variables.meta();

    ASSERT_TRUE(s(ctx));
    EXPECT_EQ(0, block_rec.get_value<std::int32_t>(block_rec_meta->value_offset(0)));
    EXPECT_EQ(1, block_rec.get_value<std::int32_t>(block_rec_meta->value_offset(1)));
    EXPECT_EQ(2, block_rec.get_value<std::int32_t>(block_rec_meta->value_offset(2)));

    ASSERT_TRUE(s(ctx));
    EXPECT_EQ(0, block_rec.get_value<std::int32_t>(block_rec_meta->value_offset(0)));
    EXPECT_EQ(2, block_rec.get_value<std::int32_t>(block_rec_meta->value_offset(1)));
    EXPECT_EQ(4, block_rec.get_value<std::int32_t>(block_rec_meta->value_offset(2)));

    ASSERT_FALSE(s(ctx));
}

}

