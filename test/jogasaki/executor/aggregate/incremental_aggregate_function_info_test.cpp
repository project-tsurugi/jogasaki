/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <gtest/gtest.h>

#include <takatori/util/fail.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/function/incremental/aggregate_function_info.h>
#include <jogasaki/executor/function/incremental/aggregate_function_kind.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::function::incremental {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace executor;
using namespace takatori::util;

class incremental_aggregate_function_info_test : public ::testing::Test {};

using kind = aggregate_function_kind;

TEST_F(incremental_aggregate_function_info_test, simple) {
    aggregate_function_info_impl<kind::sum> info{};
    auto&& pre = info.pre();
    auto&& mid = info.mid();
    auto&& post = info.post();
    EXPECT_EQ(1, pre.size());
    EXPECT_EQ(1, mid.size());
    EXPECT_EQ(1, post.size());
    EXPECT_EQ(kind::sum, info.kind());

}

TEST_F(incremental_aggregate_function_info_test, repo) {
    auto& repo = global::incremental_aggregate_function_repository();
    repo.add(0, std::make_shared<aggregate_function_info_impl<aggregate_function_kind::sum>>());
    auto& info = *repo.find(0);
    auto&& pre = info.pre();
    auto&& mid = info.mid();
    auto&& post = info.post();
    EXPECT_EQ(1, pre.size());
    EXPECT_EQ(1, mid.size());
    EXPECT_EQ(1, post.size());
    EXPECT_EQ(kind::sum, info.kind());
}
}

