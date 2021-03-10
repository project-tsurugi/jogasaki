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
#include <jogasaki/executor/function/aggregate_function_info.h>

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/global.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/executor/function/builtin_functions.h>

namespace jogasaki::executor::function {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace executor;
using namespace takatori::util;

class aggregate_function_info_test : public ::testing::Test {};

using kind = aggregate_function_kind;

TEST_F(aggregate_function_info_test, simple) {
    aggregate_function_info info{
        aggregate_function_kind::count_distinct,
        zero_generator,
        builtin::count_distinct
    };
}

TEST_F(aggregate_function_info_test, repo) {
    auto& repo = global::aggregate_function_repository();
    repo.add(0, std::make_shared<aggregate_function_info>(
        aggregate_function_kind::count_distinct,
        zero_generator,
        builtin::count_distinct
    ));
    auto& info = *repo.find(0);
    EXPECT_EQ(kind::count_distinct, info.kind());
}

}

