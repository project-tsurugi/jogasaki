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
#include <jogasaki/executor/process/impl/ops/offer.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/offer_context.h>

#include <jogasaki/basic_record.h>

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

class offer_test : public test_root {};

TEST_F(offer_test, simple) {
    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();

    std::shared_ptr<storage::table> t0 = storages->add_table("T0", {
        "T0",
        {
            { "C0", t::int4() },
            { "C1", t::int4() },
            { "C2", t::int4() },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<storage::index> i0 = storages->add_index("I0", { t0, "I0", });

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

    auto variable_mapping = std::make_shared<yugawara::analyzer::variable_mapping>();
    variable_mapping->bind(c0, t::int4{});
    variable_mapping->bind(c1, t::int4{});
    variable_mapping->bind(c2, t::int4{});
    variable_mapping->bind(f1c0, t::int4{});
    variable_mapping->bind(f1c1, t::int4{});
    variable_mapping->bind(f1c2, t::int4{});
    variable_mapping->bind(bindings(t0c0), t::int4{});
    variable_mapping->bind(bindings(t0c1), t::int4{});
    variable_mapping->bind(bindings(t0c2), t::int4{});
    yugawara::compiled_info info{{}, variable_mapping};

    processor_info pinfo{p0.operators(), info};

    std::vector<variable, takatori::util::object_allocator<variable>> columns{f1c0, f1c1, f1c2};
    variable_order order{
        variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
        columns
    };

    offer s{pinfo, r1, order, {
        {c0, f1c0},
        {c1, f1c1},
        {c2, f1c2},
    }};

    auto& block_info = pinfo.blocks_info()[s.block_index()];
    offer_context ctx(s.meta(), block_info);
    s(ctx);
}

}

