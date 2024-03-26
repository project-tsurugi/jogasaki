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
#include <memory>
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/descriptor_kind.h>
#include <takatori/plan/step_kind.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/exception.h>
#include <takatori/util/fail.h>
#include <takatori/util/string_builder.h>
#include <takatori/value/value_kind.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/storage/table.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/plan/ordered_variable_set.h>
#include <jogasaki/test_root.h>

namespace jogasaki::plan {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;
using namespace yugawara::binding;
using namespace yugawara::storage;

namespace t = ::takatori::type;
namespace v = ::takatori::value;
namespace descriptor = ::takatori::descriptor;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace plan = ::takatori::plan;
namespace binding = ::yugawara::binding;

using ::takatori::util::downcast;
using ::takatori::util::string_builder;

using varref = scalar::variable_reference;

using ::takatori::util::fail;

class ordered_variable_set_test : public test_root {

};

TEST_F(ordered_variable_set_test, basic) {
    factory f;

    ordered_variable_set variables{};
    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");
    auto&& e0c0 = f.exchange_column("e0c0");
    auto&& e0c1 = f.exchange_column("e0c1");
    auto&& e0c2 = f.exchange_column("e0c2");

    auto provider = std::make_shared<configurable_provider>();

    std::shared_ptr<::yugawara::storage::table> t0 = provider->add_table({
        "T0",
        {
            { "C0", t::int8() },
            { "C1", t::float8 () },
        },
    });
    auto&& t0c0 = f.table_column(t0->columns()[0]);
    auto&& t0c1 = f.table_column(t0->columns()[1]);
    variables.add(c0);
    variables.add(c1);
    variables.add(c2);
    variables.add(c1);
    ASSERT_EQ(3, variables.size());
    EXPECT_EQ(0, variables.index(c0));
    EXPECT_EQ(1, variables.index(c1));
    EXPECT_EQ(2, variables.index(c2));
    variables.add(e0c0);
    variables.add(e0c1);
    variables.add(e0c2);
    variables.add(e0c2);
    variables.add(t0c0);
    variables.add(t0c1);
    variables.add(t0c1);
    variables.add(t0c1);
    ASSERT_EQ(8, variables.size());
    EXPECT_EQ(3, variables.index(e0c0));
    EXPECT_EQ(4, variables.index(e0c1));
    EXPECT_EQ(5, variables.index(e0c2));
    EXPECT_EQ(6, variables.index(t0c0));
    EXPECT_EQ(7, variables.index(t0c1));
    EXPECT_TRUE(variables.remove(t0c1));
    EXPECT_FALSE(variables.remove(t0c1));
    ASSERT_EQ(7, variables.size());
}

}

