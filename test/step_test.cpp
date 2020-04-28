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
#include <isolated_process.h>
#include "test_root.h"

namespace dc::executor {
using namespace std::literals::string_literals;
using namespace dc;
using namespace dc::model;
using namespace dc::executor;
using namespace dc::executor::exchange;
using namespace dc::scheduler;

class step_test : public test_root {};

TEST_F(step_test, basic) {
    auto g = std::make_unique<common::graph>();
    auto p = std::make_unique<test::isolated_process>();
    p->activate();
    g->insert(std::move(p));
    for(auto&& v : g->steps()) {
        for(auto&& t : v->create_tasks()) {
            t->operator()();
        }
    }
    ASSERT_TRUE(true);
}

TEST_F(step_test, simple_forward) {
    auto g = std::make_unique<common::graph>();
    auto p = std::make_unique<test::isolated_process>();
    g->insert(std::move(p));
    for(auto&& v : g->steps()) {
        for(auto&& t : v->create_tasks()) {
            t->operator()();
        }
    }
    ASSERT_TRUE(true);
}

TEST_F(step_test, cogroup) {
    auto g = std::make_unique<common::graph>();
    auto scan1 = std::make_unique<simple_scan_process>();
    auto scan2 = std::make_unique<simple_scan_process>();
    auto xch1 = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0});
    auto xch2 = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0});
    auto cgrp = std::make_unique<simple_cogroup_process>();
    auto dvr = std::make_unique<deliver::step>();
    *scan1 >> *xch1;
    *scan2 >> *xch2;
    *xch1 >> *cgrp;
    *xch2 >> *cgrp;
    *cgrp >> *dvr;
    // step id are assigned from 0 to 5
    g->insert(std::move(scan1));
    g->insert(std::move(xch1));
    g->insert(std::move(scan2));
    g->insert(std::move(xch2));
    g->insert(std::move(cgrp));
    g->insert(std::move(dvr));
    dag_controller dc{};
    dc.schedule(*g);
    ASSERT_TRUE(true);
}

}
