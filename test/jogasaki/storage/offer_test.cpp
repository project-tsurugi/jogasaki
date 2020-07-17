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
#include <jogasaki/executor/process/impl/relop/offer.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/relop/offer_context.h>

#include <jogasaki/basic_record.h>

namespace jogasaki::executor::process::impl::relop {

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

using rgraph = ::takatori::graph::graph<relation::expression>;

class offer_test : public test_root {};

TEST_F(offer_test, simple) {
    yugawara::binding::factory bindings;
    rgraph rg;

    auto&& c1 = bindings.stream_variable("c1");
    auto&& c2 = bindings.stream_variable("c2");
    auto&& c3 = bindings.stream_variable("c3");

    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };

    auto&& r2 = rg.insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c1, f1.columns()[0] },
            { c2, f1.columns()[1] },
            { c3, f1.columns()[2] },
        },
    });

    yugawara::compiled_info info{};
    processor_info pinfo{rg, info};

    offer s{pinfo, r2, {}, {}};
    offer_context ctx(s.meta());
//    s(ctx);
}

}

