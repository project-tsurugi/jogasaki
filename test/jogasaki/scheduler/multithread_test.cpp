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

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/mock/simple_cogroup_process.h>
#include <jogasaki/mock/simple_emit_process.h>
#include <jogasaki/mock/simple_scan_process.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/test_root.h>

namespace jogasaki::scheduler {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::common;
using namespace jogasaki::scheduler;

class multithread_test : public test_root {};

TEST_F(multithread_test, DISABLED_simple_forward) {
    auto cfg = std::make_shared<configuration>();
    cfg->thread_pool_size(1);
    cfg->single_thread(false);
    global::config_pool(cfg);
    auto ctx = std::make_shared<request_context>();
    auto g = std::make_shared<common::graph>();
    auto scan = std::make_unique<simple_scan_process>();
    auto emit = std::make_unique<simple_emit_process>();
    auto fwd = std::make_unique<forward::step>();
    auto fwd2 = std::make_unique<forward::step>();
    *scan >> *fwd;
    *fwd >> *emit;
    *emit >> *fwd2;
    g->insert(std::move(scan));
    g->insert(std::move(fwd));
    g->insert(std::move(emit));
    g->insert(std::move(fwd2));
    statement_scheduler dc{cfg};
    job_context jctx{};
    ctx->job(maybe_shared_ptr{&jctx});
    ctx->stmt_scheduler(maybe_shared_ptr{&dc});
    dc.schedule(execute{g}, *ctx);
    ASSERT_TRUE(true);
}

TEST_F(multithread_test, DISABLED_simple_shuffle) {
    auto cfg = std::make_shared<configuration>();
    cfg->thread_pool_size(1);
    cfg->single_thread(false);
    global::config_pool(cfg);
    auto ctx = std::make_shared<request_context>();
    auto g = std::make_shared<common::graph>();
    auto scan = std::make_unique<simple_scan_process>();
    auto emit = std::make_unique<simple_emit_process>();
    auto xch = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0}, meta::variable_order{}, meta::variable_order{});
    auto fwd = std::make_unique<forward::step>();
    *scan >> *xch;
    *xch >> *emit;
    *emit >> *fwd;
    g->insert(std::move(scan));
    g->insert(std::move(xch));
    g->insert(std::move(emit));
    g->insert(std::move(fwd));
    statement_scheduler dc{cfg};
    job_context jctx{};
    ctx->job(maybe_shared_ptr{&jctx});
    ctx->stmt_scheduler(maybe_shared_ptr{&dc});
    dc.schedule(execute{g}, *ctx);
    ASSERT_TRUE(true);
}

TEST_F(multithread_test, DISABLED_cogroup) {
    auto cfg = std::make_shared<configuration>();
    cfg->thread_pool_size(1);
    cfg->single_thread(false);
    global::config_pool(cfg);
    auto ctx = std::make_shared<request_context>();
    auto g = std::make_shared<common::graph>();
    auto scan1 = std::make_unique<simple_scan_process>();
    auto scan2 = std::make_unique<simple_scan_process>();
    auto xch1 = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0}, meta::variable_order{}, meta::variable_order{});
    auto xch2 = std::make_unique<group::step>(test_record_meta1(), std::vector<std::size_t>{0}, meta::variable_order{}, meta::variable_order{});
    auto cgrp = std::make_unique<simple_cogroup_process>();
    auto fwd = std::make_unique<forward::step>();
    *scan1 >> *xch1;
    *scan2 >> *xch2;
    *xch1 >> *cgrp;
    *xch2 >> *cgrp;
    *cgrp >> *fwd;
    // step id are assigned from 0 to 5
    g->insert(std::move(scan1));
    g->insert(std::move(xch1));
    g->insert(std::move(scan2));
    g->insert(std::move(xch2));
    g->insert(std::move(cgrp));
    g->insert(std::move(fwd));

    statement_scheduler dc{cfg};
    job_context jctx{};
    ctx->job(maybe_shared_ptr{&jctx});
    ctx->stmt_scheduler(maybe_shared_ptr{&dc});
    dc.schedule(execute{g}, *ctx);
    ASSERT_TRUE(true);
}

}
