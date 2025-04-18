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
#include <jogasaki/transaction_state.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cxxabi.h>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/error_info.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/logging.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::api {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using impl::get_impl;

/**
 * @brief test transaction state
 */
class transaction_state_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(transaction_state_test, allowed_state_transition) {
    // verify only the typical transitions
    using kind = transaction_state_kind;
    using namespace jogasaki::details;
    EXPECT_TRUE(transition_allowed(kind::undefined, kind::init));
    EXPECT_TRUE(transition_allowed(kind::init, kind::active));
    EXPECT_TRUE(transition_allowed(kind::init, kind::aborted));
    EXPECT_TRUE(transition_allowed(kind::active, kind::going_to_abort));
    EXPECT_TRUE(transition_allowed(kind::active, kind::going_to_commit));
    EXPECT_TRUE(transition_allowed(kind::active, kind::aborted));
    EXPECT_TRUE(transition_allowed(kind::active, kind::unknown));
    EXPECT_TRUE(transition_allowed(kind::going_to_commit, kind::cc_committing));
    EXPECT_TRUE(transition_allowed(kind::going_to_abort, kind::aborted));
    EXPECT_TRUE(transition_allowed(kind::cc_committing, kind::committed_available));
    EXPECT_TRUE(transition_allowed(kind::cc_committing, kind::committed_stored));
    EXPECT_TRUE(transition_allowed(kind::cc_committing, kind::aborted));
}

TEST_F(transaction_state_test, disallowed_state_transition) {
    // verify disallowed transitions that are clearly invalid
    using kind = transaction_state_kind;
    using namespace jogasaki::details;
    EXPECT_TRUE(! transition_allowed(kind::init, kind::undefined));
    EXPECT_TRUE(! transition_allowed(kind::active, kind::init));
    EXPECT_TRUE(! transition_allowed(kind::going_to_commit, kind::active));
    EXPECT_TRUE(! transition_allowed(kind::going_to_commit, kind::going_to_abort));

    EXPECT_TRUE(! transition_allowed(kind::going_to_abort, kind::active));
    EXPECT_TRUE(! transition_allowed(kind::going_to_abort, kind::going_to_commit));
    EXPECT_TRUE(! transition_allowed(kind::going_to_abort, kind::cc_committing));
    EXPECT_TRUE(! transition_allowed(kind::going_to_abort, kind::committed_available));
    EXPECT_TRUE(! transition_allowed(kind::going_to_abort, kind::committed_stored));

    EXPECT_TRUE(! transition_allowed(kind::cc_committing, kind::active));
    EXPECT_TRUE(! transition_allowed(kind::cc_committing, kind::going_to_commit));
    EXPECT_TRUE(! transition_allowed(kind::cc_committing, kind::going_to_abort));
    EXPECT_TRUE(! transition_allowed(kind::committed_available, kind::active));
    EXPECT_TRUE(! transition_allowed(kind::committed_available, kind::going_to_commit));
    EXPECT_TRUE(! transition_allowed(kind::committed_available, kind::going_to_abort));
    EXPECT_TRUE(! transition_allowed(kind::committed_available, kind::cc_committing));
    EXPECT_TRUE(! transition_allowed(kind::committed_available, kind::aborted));
    EXPECT_TRUE(! transition_allowed(kind::committed_stored, kind::active));
    EXPECT_TRUE(! transition_allowed(kind::committed_stored, kind::going_to_commit));
    EXPECT_TRUE(! transition_allowed(kind::committed_stored, kind::going_to_abort));
    EXPECT_TRUE(! transition_allowed(kind::committed_stored, kind::cc_committing));
    EXPECT_TRUE(! transition_allowed(kind::committed_stored, kind::aborted));
    EXPECT_TRUE(! transition_allowed(kind::committed_stored, kind::committed_available));
    EXPECT_TRUE(! transition_allowed(kind::aborted, kind::active));
    EXPECT_TRUE(! transition_allowed(kind::aborted, kind::going_to_commit));
    EXPECT_TRUE(! transition_allowed(kind::aborted, kind::going_to_abort));
    EXPECT_TRUE(! transition_allowed(kind::aborted, kind::cc_committing));
    EXPECT_TRUE(! transition_allowed(kind::aborted, kind::committed_available));
    EXPECT_TRUE(! transition_allowed(kind::aborted, kind::committed_stored));
}

TEST_F(transaction_state_test, init) {
    auto& db = get_impl(*db_);
    transaction_option opts{false, false};
    transaction_handle tx{};
    ASSERT_EQ(status::ok, db.create_transaction_internal(tx, opts));
    auto tctx = get_transaction_context(tx);
    EXPECT_EQ(transaction_state_kind::init, tctx->state());
    ASSERT_EQ(status::ok, tx.commit());
}

TEST_F(transaction_state_test, occ_active) {
    auto tx = utils::create_transaction(*db_, false, false);
    auto tctx = get_transaction_context(*tx);
    EXPECT_EQ(transaction_state_kind::active, tctx->state());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(transaction_state_test, ltx_active) {
    auto tx = utils::create_transaction(*db_, false, true);
    auto tctx = get_transaction_context(*tx);
    EXPECT_EQ(transaction_state_kind::active, tctx->state());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(transaction_state_test, rtx_active) {
    auto tx = utils::create_transaction(*db_, true, false);
    auto tctx = get_transaction_context(*tx);
    EXPECT_EQ(transaction_state_kind::active, tctx->state());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(transaction_state_test, commit) {
    // verify the state after commit request
    // we cannot stop in the middle of commit, so expect either the status going_to_commit through committed_stored
    auto tx = utils::create_transaction(*db_, false, false);
    auto tctx = get_transaction_context(*tx);
    ASSERT_EQ(status::ok, tx->commit());

    transaction_state_kind_set expected{
        transaction_state_kind::going_to_commit,
        transaction_state_kind::cc_committing,
        transaction_state_kind::committed_available,
        transaction_state_kind::committed_stored,
    };
    EXPECT_TRUE(expected.contains(tctx->state())) << tctx->state();
}

TEST_F(transaction_state_test, abort) {
    // verify the state after abort request
    // we cannot stop in the middle of abort, so expect either the status going_to_abort or aborted
    auto tx = utils::create_transaction(*db_, false, false);
    auto tctx = get_transaction_context(*tx);
    ASSERT_EQ(status::ok, tx->abort());
    transaction_state_kind_set expected{
        transaction_state_kind::going_to_abort,
        transaction_state_kind::aborted,
    };
    EXPECT_TRUE(expected.contains(tctx->state())) << tctx->state();
}

TEST_F(transaction_state_test, early_abort_by_cc) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory does not raise early abort";
    }
    execute_statement("create table t (c0 int primary key)");
    auto tx = utils::create_transaction(*db_, false, true);
    auto tctx = get_transaction_context(*tx);
    test_stmt_err("insert into t values (1)", *tx, error_code::ltx_write_operation_without_write_preserve_exception);

    EXPECT_EQ(transaction_state_kind::aborted, tctx->state());
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(transaction_state_test, early_abort_by_sql) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1)");
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);
    test_stmt_err("insert into t values (1)", *tx, error_code::unique_constraint_violation_exception);

    EXPECT_EQ(transaction_state_kind::aborted, tctx->state());
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(transaction_state_test, abort_after_early_abort_by_cc) {
    // verify the state won't go back to going_to_abort after cc early abort
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory does not raise early abort";
    }
    execute_statement("create table t (c0 int primary key)");
    auto tx = utils::create_transaction(*db_, false, true);
    auto tctx = get_transaction_context(*tx);
    test_stmt_err("insert into t values (1)", *tx, error_code::ltx_write_operation_without_write_preserve_exception);

    EXPECT_EQ(transaction_state_kind::aborted, tctx->state());
    ASSERT_EQ(status::ok, tx->abort());
    EXPECT_EQ(transaction_state_kind::aborted, tctx->state());
}

}

