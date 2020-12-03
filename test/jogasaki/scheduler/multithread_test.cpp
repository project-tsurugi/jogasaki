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

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/exchange/deliver/step.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/common/task.h>

#include <jogasaki/mock/simple_scan_process.h>
#include <jogasaki/mock/simple_cogroup_process.h>
#include <jogasaki/mock/simple_emit_process.h>
#include <jogasaki/test_root.h>

namespace jogasaki::scheduler {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::scheduler;

class multithread_test : public test_root {};

TEST_F(multithread_test, simple_forward) {
    auto ctx = std::make_shared<request_context>();
    auto g = std::make_unique<common::graph>(*ctx);
    auto scan = std::make_unique<simple_scan_process>();
    auto emit = std::make_unique<simple_emit_process>();
    auto fwd = std::make_unique<forward::step>();
    auto dvr = std::make_unique<deliver::step>();
    *scan >> *fwd;
    *fwd >> *emit;
    *emit >> *dvr;
    g->insert(std::move(scan));
    g->insert(std::move(fwd));
    g->insert(std::move(emit));
    g->insert(std::move(dvr));
    auto cfg = std::make_shared<configuration>();
    cfg->thread_pool_size(1);
    cfg->single_thread(false);
    dag_controller dc{cfg};
    dc.schedule(*g);
    ASSERT_TRUE(true);
}

TEST_F(multithread_test, simple_shuffle) {
    auto ctx = std::make_shared<request_context>();
    auto g = std::make_unique<common::graph>(*ctx);
    auto scan = std::make_unique<simple_scan_process>();
    auto emit = std::make_unique<simple_emit_process>();
    auto xch = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0}, meta::variable_order{}, meta::variable_order{});
    auto dvr = std::make_unique<deliver::step>();
    *scan >> *xch;
    *xch >> *emit;
    *emit >> *dvr;
    g->insert(std::move(scan));
    g->insert(std::move(xch));
    g->insert(std::move(emit));
    g->insert(std::move(dvr));
    auto cfg = std::make_shared<configuration>();
    cfg->thread_pool_size(1);
    cfg->single_thread(false);
    dag_controller dc{cfg};
    dc.schedule(*g);
    ASSERT_TRUE(true);
}

TEST_F(multithread_test, cogroup) {
    auto ctx = std::make_shared<request_context>();
    auto g = std::make_unique<common::graph>(*ctx);
    auto scan1 = std::make_unique<simple_scan_process>();
    auto scan2 = std::make_unique<simple_scan_process>();
    auto xch1 = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0}, meta::variable_order{}, meta::variable_order{});
    auto xch2 = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0}, meta::variable_order{}, meta::variable_order{});
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

    auto cfg = std::make_shared<configuration>();
    cfg->thread_pool_size(1);
    cfg->single_thread(false);
    dag_controller dc{cfg};
    dc.schedule(*g);
    ASSERT_TRUE(true);
}

}
