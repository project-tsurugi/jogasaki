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
#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/plan/process.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/utils/relation_indices.h>


namespace jogasaki::utils {

class relation_indices_test : public ::testing::Test {};

TEST_F(relation_indices_test, input) {

    yugawara::binding::factory bindings;
    takatori::plan::forward f0 {};
    takatori::plan::forward f1 {};
    takatori::plan::forward f2 {};
    auto fwd0 = bindings(f0);
    auto fwd1 = bindings(f1);
    auto fwd2 = bindings(f2);

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});

    f0.add_downstream(p0);
    f1.add_downstream(p0);
    f2.add_downstream(p0);

    EXPECT_EQ(0, find_input_index(p0, fwd0));
    EXPECT_EQ(1, find_input_index(p0, fwd1));
    EXPECT_EQ(2, find_input_index(p0, fwd2));
}

TEST_F(relation_indices_test, output) {

    yugawara::binding::factory bindings;
    takatori::plan::forward f0 {};
    takatori::plan::forward f1 {};
    takatori::plan::forward f2 {};
    auto fwd0 = bindings(f0);
    auto fwd1 = bindings(f1);
    auto fwd2 = bindings(f2);

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});

    f0.add_upstream(p0);
    f1.add_upstream(p0);
    f2.add_upstream(p0);

    EXPECT_EQ(0, find_output_index(p0, fwd0));
    EXPECT_EQ(1, find_output_index(p0, fwd1));
    EXPECT_EQ(2, find_output_index(p0, fwd2));
}

}

