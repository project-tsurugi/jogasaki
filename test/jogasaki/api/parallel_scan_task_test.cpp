/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"


namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class parallel_scan_task_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->rtx_parallel_scan(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(parallel_scan_task_test, simple) {
    // manually verify the log message and check pivots for stored data
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (100), (200), (300)");

    std::unique_ptr<api::executable_statement> stmt{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::ok, api::impl::get_impl(*db_).create_executable("select * from t", stmt, info));

    auto s = static_cast<api::impl::executable_statement*>(stmt.get())->body();
    ASSERT_TRUE(s->is_execute());
    auto* e = static_cast<executor::common::execute*>(s->operators().get());
    auto& g = e->operators();
    ASSERT_EQ(1, g.size());
    auto& p = static_cast<executor::process::step&>(*g.steps()[0]);

    auto tx = utils::create_transaction(*db_, true, false);
    auto tctx = get_transaction_context(*tx);

    auto rctx = api::impl::create_request_context(
        api::impl::get_impl(*db_),
        tctx,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {},
        nullptr
    );

    auto& sc = scheduler::statement_scheduler::impl::get_impl(*rctx->stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.init(g, *rctx);

    p.activate(*rctx);

    auto tasks = p.create_tasks(*rctx);
    ASSERT_LT(0, tasks.size());
    EXPECT_EQ(task_transaction_kind::in_transaction, tasks[0]->transaction_capability());

    ASSERT_EQ(status::ok, tx->commit());
}

} // namespace jogasaki::testing
