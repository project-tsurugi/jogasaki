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
#include <decimal.hh>
#include <iostream>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

namespace jogasaki::executor::process::impl::expression {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;

using namespace testing;

using accessor::text;
using takatori::decimal::triple;

class decimal_triple_conv_test : public test_root {
public:
    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};
};

TEST_F(decimal_triple_conv_test, as_triple) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    decimal::Decimal d{triple{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}};
    std::cerr << d.to_sci() << std::endl;
    d = d + 1;
    std::cerr << d.to_sci() << std::endl;
    auto a = details::as_triple(d, ctx);
    EXPECT_TRUE(a);
}

}

