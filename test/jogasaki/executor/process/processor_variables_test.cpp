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
#include <jogasaki/executor/process/impl/processor_variables.h>

#include <string>

#include <yugawara/binding/factory.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <takatori/plan/forward.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/step/take_flat.h>

namespace jogasaki::executor::process::impl {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;
using namespace yugawara::binding;

class processor_variables_test : public test_root {

};

namespace relation = ::takatori::relation;
//namespace scalar = ::takatori::scalar;
using take = relation::step::take_flat;
using offer = relation::step::offer;
using buffer = relation::buffer;

using rgraph = ::takatori::graph::graph<relation::expression>;

TEST_F(processor_variables_test, DISABLED_basic) {
    factory f;
    ::takatori::plan::forward f1 {
        f.exchange_column(),
        f.exchange_column(),
        f.exchange_column(),
    };
    ::takatori::plan::forward f2 {
        f.exchange_column(),
        f.exchange_column(),
        f.exchange_column(),
    };

    rgraph rg;

    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");
    auto&& c3 = f.stream_variable("c3");
    auto&& r1 = rg.insert(take {
        f.exchange(f1),
        {
            { f1.columns()[0], c1 },
            { f1.columns()[1], c2 },
            { f1.columns()[2], c3 },
        },
    });
    auto&& r2 = rg.insert(offer {
        f.exchange(f2),
        {
            { c2, f2.columns()[0] },
            { c1, f2.columns()[1] },
            { c1, f2.columns()[2] },
        },
    });
    r1.output() >> r2.input();

    auto expression_mapping = std::make_shared<yugawara::analyzer::expression_mapping const>();
    auto variable_mapping = std::make_shared<yugawara::analyzer::variable_mapping const>();
    yugawara::compiled_info info{expression_mapping, variable_mapping};

    auto pinfo = std::make_shared<processor_info>(rg, info);
    processor_variables v{pinfo};

    ASSERT_EQ(1, v.block_variables().size());
    auto& b = v.block_variables()[0];
    auto meta = b.meta();
    ASSERT_EQ(2, meta->field_count());
}

}

