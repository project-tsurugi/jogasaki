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

#include <gtest/gtest.h>
#include <executor/common/graph.h>
#include <mock/simple_scan_process.h>
#include <mock/simple_cogroup_process.h>
#include <scheduler/dag_controller.h>
#include <executor/exchange/deliver/step.h>
#include <test_process.h>
#include "test_root.h"

namespace jogasaki::executor {
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::scheduler;

class step_test : public test_root {};

TEST_F(step_test, basic) {
    common::graph g{};
    auto& p = g.emplace<test::test_process>();
    p.activate();
    for(auto&& v : g.steps()) {
        for(auto&& t : v->create_tasks()) {
            t->operator()();
        }
    }
    p.deactivate();
    ASSERT_TRUE(true);
}

TEST_F(step_test, create_find_step) {
    common::graph g{};
    auto& p0 = g.emplace<test::test_process>();
    auto& p1 = g.emplace<test::test_process>();
    auto& p2 = g.emplace<test::test_process>();

    // unique ids assigned
    ASSERT_NE(p0.id(), p1.id());
    ASSERT_NE(p1.id(), p2.id());
    ASSERT_NE(p2.id(), p0.id());

    auto step = g.find_step(p1.id());
    ASSERT_TRUE(step);
    EXPECT_EQ(*step, p1);

    auto owner = p1.owner();
    ASSERT_TRUE(owner);
    EXPECT_EQ(g, *owner);
}

TEST_F(step_test, insert_step) {
    common::graph g{};
    auto& p0 = g.insert(std::make_unique<test::test_process>());
    auto& p1 = g.insert(std::make_unique<test::test_process>());
    auto& p2 = g.insert(std::make_unique<test::test_process>());

    // unique ids assigned
    ASSERT_NE(p0.id(), p1.id());
    ASSERT_NE(p1.id(), p2.id());
    ASSERT_NE(p2.id(), p0.id());

    auto step = g.find_step(p1.id());
    ASSERT_TRUE(step);
    EXPECT_EQ(*step, p1);

    auto owner = p1.owner();
    ASSERT_TRUE(owner);
    EXPECT_EQ(g, *owner);
}

TEST_F(step_test, steps) {
    common::graph g{};
    auto& p0 = g.emplace<test::test_process>();
    auto& p1 = g.emplace<test::test_process>();
    auto& p2 = g.emplace<test::test_process>();
    std::set<model::step*> steps{};
    ASSERT_EQ(3, g.steps().size());
    for(auto&& s : g.steps()) {
        steps.emplace(s.get());
    }
    EXPECT_EQ(std::set<model::step*>({&p0, &p1, &p2}), steps);
}

TEST_F(step_test, clear) {
    common::graph g{};
    g.emplace<test::test_process>();
    g.emplace<test::test_process>();
    ASSERT_EQ(2, g.size());
    g.clear();
    ASSERT_EQ(0, g.size());
    auto v = g.steps();
    ASSERT_EQ(0, v.size());
}

TEST_F(step_test, cogroup) {
    common::graph g{};
    auto& scan1 = g.emplace<simple_scan_process>();
    auto& scan2 = g.emplace<simple_scan_process>();
    auto& xch1 = g.emplace<group::step>(test_record_meta1(), std::vector<std::size_t>{0});
    auto& xch2 = g.emplace<group::step>(test_record_meta1(), std::vector<std::size_t>{0});
    auto& cgrp = g.emplace<simple_cogroup_process>();
    auto& dvr = g.emplace<deliver::step>();
    scan1 >> xch1;
    scan2 >> xch2;
    xch1 >> cgrp;
    xch2 >> cgrp;
    cgrp >> dvr;
    dag_controller dc{};
    dc.schedule(g);
    ASSERT_TRUE(true);
}

}
