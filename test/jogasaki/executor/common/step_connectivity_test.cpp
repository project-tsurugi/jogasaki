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
#include <jogasaki/executor/common/step.h>

#include <gtest/gtest.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/exchange/deliver/step.h>

#include <jogasaki/mock/simple_scan_process.h>
#include <jogasaki/mock/simple_cogroup_process.h>
#include <jogasaki/test_process.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor {
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::scheduler;

class step_connectivity_test : public test_root {};

class test_step : public process::step {
public:
    test_step() = default;
    void activate() override {}
    //TODO fix operator<< to dump object info.
};

test_step& opposite(model::port& p) {
    if (p.opposites().empty()) fail();
    auto* r = dynamic_cast<test_step*>(p.opposites()[0]->owner());
    if (! r) fail();
    return *r;
}

TEST_F(step_connectivity_test, connect_to_left) {
    common::graph g{};
    auto& s1 = g.emplace<test_step>();
    auto& s2 = g.emplace<test_step>();
    s1 << s2;
    EXPECT_EQ(1, s1.input_ports().size());
    EXPECT_EQ(0, s1.output_ports().size());
    EXPECT_EQ(0, s2.input_ports().size());
    EXPECT_EQ(1, s2.output_ports().size());

    EXPECT_EQ(s2, opposite(*s1.input_ports()[0]));
    EXPECT_EQ(s1, opposite(*s2.output_ports()[0]));
}

TEST_F(step_connectivity_test, connect_to_right) {
    common::graph g{};
    auto& s1 = g.emplace<test_step>();
    auto& s2 = g.emplace<test_step>();
    s2 >> s1;
    EXPECT_EQ(1, s1.input_ports().size());
    EXPECT_EQ(0, s1.output_ports().size());
    EXPECT_EQ(0, s2.input_ports().size());
    EXPECT_EQ(1, s2.output_ports().size());

    EXPECT_EQ(s2, opposite(*s1.input_ports()[0]));
    EXPECT_EQ(s1, opposite(*s2.output_ports()[0]));
}

TEST_F(step_connectivity_test, multiple_input) {
    common::graph g{};
    auto& s0 = g.emplace<test_step>();
    auto& s1 = g.emplace<test_step>();
    auto& s2 = g.emplace<test_step>();
    s0 << s1;
    s0 << s2;
    EXPECT_EQ(2, s0.input_ports().size());
    EXPECT_EQ(0, s0.output_ports().size());
    EXPECT_EQ(0, s1.input_ports().size());
    EXPECT_EQ(1, s1.output_ports().size());
    EXPECT_EQ(0, s2.input_ports().size());
    EXPECT_EQ(1, s2.output_ports().size());

    EXPECT_EQ(s1, opposite(*s0.input_ports()[0]));
    EXPECT_EQ(s2, opposite(*s0.input_ports()[1]));
    EXPECT_EQ(s0, opposite(*s1.output_ports()[0]));
    EXPECT_EQ(s0, opposite(*s2.output_ports()[0]));
}
TEST_F(step_connectivity_test, multiple_output) {
    common::graph g{};
    auto& s0 = g.emplace<test_step>();
    auto& s1 = g.emplace<test_step>();
    auto& s2 = g.emplace<test_step>();
    s1 << s0;
    s2 << s0;
    EXPECT_EQ(0, s0.input_ports().size());
    EXPECT_EQ(2, s0.output_ports().size());
    EXPECT_EQ(1, s1.input_ports().size());
    EXPECT_EQ(0, s1.output_ports().size());
    EXPECT_EQ(1, s2.input_ports().size());
    EXPECT_EQ(0, s2.output_ports().size());

    EXPECT_EQ(s1, opposite(*s0.output_ports()[0]));
    EXPECT_EQ(s2, opposite(*s0.output_ports()[1]));
    EXPECT_EQ(s0, opposite(*s1.input_ports()[0]));
    EXPECT_EQ(s0, opposite(*s2.input_ports()[0]));
}

TEST_F(step_connectivity_test, connect_mutiple_steps_at_once) {
    common::graph g{};
    auto& scan1 = g.emplace<test_step>();
    auto& scan2 = g.emplace<test_step>();
    auto& xch1 = g.emplace<test_step>();
    auto& xch2 = g.emplace<test_step>();
    auto& cgrp = g.emplace<test_step>();
    auto& dvr = g.emplace<test_step>();
    scan1 >> xch1 >> cgrp >> dvr;
    cgrp << xch2 << scan2;
    EXPECT_EQ(0, scan1.input_ports().size());
    EXPECT_EQ(1, scan1.output_ports().size());
    EXPECT_EQ(0, scan2.input_ports().size());
    EXPECT_EQ(1, scan2.output_ports().size());
    EXPECT_EQ(1, xch1.input_ports().size());
    EXPECT_EQ(1, xch1.output_ports().size());
    EXPECT_EQ(1, xch2.input_ports().size());
    EXPECT_EQ(1, xch2.output_ports().size());
    EXPECT_EQ(2, cgrp.input_ports().size());
    EXPECT_EQ(1, cgrp.output_ports().size());
    EXPECT_EQ(1, dvr.input_ports().size());
    EXPECT_EQ(0, dvr.output_ports().size());
}

}
