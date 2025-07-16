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
 * @brief test database api
 */
class transaction_test :
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

        auto* impl = db_impl();
        utils::add_benchmark_tables(*impl->tables());
        utils::add_test_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(transaction_test, concurrent_query_requests_on_same_tx) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::unique_ptr<api::executable_statement> stmt0{}, stmt1{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 ORDER BY C0", stmt0));
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 ORDER BY C0", stmt1));
    auto tx = utils::create_transaction(*db_);
    status s{};
    std::string message{"message"};
    std::atomic_bool run0{false}, run1{false}, error_abort{false};
    test_channel ch0{}, ch1{};
    ASSERT_TRUE(tx->execute_async(
        maybe_shared_ptr{stmt0.get()},
        maybe_shared_ptr{&ch0},
        [&](status st, std::string_view msg){
            if(st != status::ok) {
                LOG(ERROR) << st;
                error_abort.store(true);
            }
            run0.store(true);
        }
    ));
    ASSERT_TRUE(tx->execute_async(
        maybe_shared_ptr{stmt1.get()},
        maybe_shared_ptr{&ch1},
        [&](status st, std::string_view msg){
            if(st != status::ok) {
                LOG(ERROR) << st;
                error_abort.store(true);
            }
            run1.store(true);
        }
    ));
    while(! error_abort.load() && !(run0.load() && run1.load())) {}
    if(error_abort) {
        // to continue test in-case err_not_implemented
        FAIL();
    }
    {
        auto& wrt = ch0.writers_[0];
        ASSERT_TRUE(stmt0->meta());
        auto& m = *unsafe_downcast<api::impl::record_meta>(stmt0->meta());
        auto recs = utils::deserialize_msg({wrt->data_.data(), wrt->size_}, *m.meta());
        ASSERT_EQ(3, recs.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), recs[0]);
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 20.0)), recs[1]);
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 30.0)), recs[2]);
        EXPECT_TRUE(ch0.all_writers_released());
    }
    {
        auto& wrt = ch1.writers_[0];
        ASSERT_TRUE(stmt1->meta());
        auto& m = *unsafe_downcast<api::impl::record_meta>(stmt1->meta());
        auto recs = utils::deserialize_msg({wrt->data_.data(), wrt->size_}, *m.meta());
        ASSERT_EQ(3, recs.size());
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), recs[0]);
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 20.0)), recs[1]);
        EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 30.0)), recs[2]);
        EXPECT_TRUE(ch1.all_writers_released());
    }
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(transaction_test, readonly_option) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", *tx, result);
    EXPECT_EQ(1, result.size());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(transaction_test, tx_destroyed_while_query_is_still_running) {
    utils::set_global_tx_option(utils::create_tx_option{false, true}); // use occ to finish insert quickly
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    for(std::size_t i=0; i < 100; ++i) {
        execute_statement("INSERT INTO T VALUES ("+std::to_string(i)+")");
    }

    std::unique_ptr<api::executable_statement> stmt0{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T ORDER BY C0", stmt0));

    test_channel ch0{};
    std::atomic_bool run0{false};
    api::transaction_handle tx{};
    ASSERT_EQ(status::ok, db_->create_transaction(tx));
    ASSERT_TRUE(tx.execute_async(
        maybe_shared_ptr{stmt0.get()},
        maybe_shared_ptr{&ch0},
        [&](status st, std::string_view msg) {
            VLOG(log_info) << "**** query completed ***";
            if (st != status::ok) {
                LOG(ERROR) << st;
            }
            run0 = true;
        }
    ));
    ASSERT_EQ(status::ok, db_->destroy_transaction(tx));
    VLOG(log_info) << "**** destroying tx completed ***";
    while(! run0.load()) {}
    ASSERT_EQ(0, get_impl(*db_).transaction_count());
}

using callback_type = std::function<void(std::size_t)>;

void execute_n(
    std::function<void(std::size_t, callback_type)> f,
    std::function<void(void)> finally,
    std::size_t n
) {
    const auto cb = [f, finally, n](std::size_t i) {
        auto cb_impl = [f, finally, n](std::size_t k, auto& cb_ref) -> void {
            if(k+1 < n) {
                f(k+1, [cb_ref](std::size_t j) {
                    cb_ref(j, cb_ref);
                });
            } else {
                finally();
            }
        };
        cb_impl(i, cb_impl);
    };
    f(0, cb);
}

TEST_F(transaction_test, execute_n) {
    // test execute_n utility function
    std::vector<std::size_t> result{};
    std::atomic_bool called = false;
    execute_n(
        [&](std::size_t i, callback_type cb) {
            ASSERT_EQ(i, result.size());
            result.emplace_back(i);
            cb(i);
        },
        [&]() {
            called = true;
            ASSERT_EQ(10, result.size());
            ASSERT_EQ(0, result[0]);
            ASSERT_EQ(9, result[9]);
        },
        10
    );
    ASSERT_TRUE(called);
}

TEST_F(transaction_test, tx_destroyed_from_other_threads) {
    // verify crash doesn't occur even tx handle is destroyed suddenly by the other threads
    utils::set_global_tx_option(utils::create_tx_option{false, true}); // use occ to finish insert quickly
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    for(std::size_t i=0; i < 3; ++i) {
        execute_statement("INSERT INTO T VALUES ("+std::to_string(i)+")");
    }

    std::unique_ptr<api::executable_statement> stmt0{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T ORDER BY C0", stmt0));

    std::atomic_bool run0{false};
    std::atomic_size_t statements_executed = 0;
    std::atomic_size_t destroyed_f1 = 0;
    std::atomic_size_t destroyed_f2 = 0;
    std::atomic_size_t execute_rejected = 0;
    std::atomic<api::transaction_handle> tx{};
    std::size_t num_statements = 100;

    auto f1 = std::async(std::launch::async, [&]() {
        // repeat create tx, execute statement, destroy tx
        execute_n(
            [&](std::size_t i, callback_type cb) {
                db_->create_transaction_async([&, cb, i](transaction_handle t, status st, std::string_view msg) {
                    tx = t;
                    std::this_thread::sleep_for(10us);
                    auto ch0 = std::make_shared<test_channel>();
                    t.execute_async(
                        maybe_shared_ptr{stmt0.get()},
                        ch0,
                        [&, ch0, i, cb, t](status st, std::string_view msg) {
                            ++statements_executed;
                            if (st != status::ok) {
                                if(st == status::err_invalid_argument) {
                                    ++execute_rejected;
                                } else {
                                    LOG(ERROR) << st;
                                }
                            }
                            if(db_->destroy_transaction(t) == status::ok) {
                                ++destroyed_f1;
                            }
                            transaction_handle tmp = t;
                            tx.compare_exchange_strong(tmp, transaction_handle{});
                            cb(i);
                        }
                    );
                });
            },
            [&]() {
                run0 = true;
            },
            num_statements
        );
    });
    auto f2 = std::async(std::launch::async, [&]() {
        while(! run0) {
            std::this_thread::sleep_for(5ms);
            if(auto t = static_cast<api::transaction_handle>(tx)) {
                if(db_->destroy_transaction(t) == status::ok) {
                    ++destroyed_f2;
                }
                tx.compare_exchange_strong(t, transaction_handle{});
            }
        }
    });
    while(! run0.load()) {}
    // manually check most are destroyed by f2, and some are execute_rejected (invalid handle)
    std::cerr << "statements_executed:" << statements_executed << std::endl;
    std::cerr << "destroyed_f1:" << destroyed_f1 << std::endl;
    std::cerr << "destroyed_f2:" << destroyed_f2 << std::endl;
    std::cerr << "execute_rejected:" << execute_rejected << std::endl;
    ASSERT_EQ(0, get_impl(*db_).transaction_count());
}

// TODO following test leaks many objects
TEST_F(transaction_test, tx_aborted_from_other_threads) {
    // verify crash doesn't occur even tx is aborted by operation on different thread
    // and the error info is available then
    utils::set_global_tx_option(utils::create_tx_option{false, true}); // use occ to finish insert quickly
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    for(std::size_t i=0; i < 5; ++i) {
        execute_statement("INSERT INTO T VALUES ("+std::to_string(i)+")");
    }

    std::unique_ptr<api::executable_statement> stmt0{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T ORDER BY C0", stmt0));

    // statement causing PK violation and tx abort
    std::unique_ptr<api::executable_statement> stmt1{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T VALUES(0)", stmt1));

    std::atomic_bool run0{false};
    std::atomic_size_t statements_executed = 0;
    std::atomic_size_t destroyed_f1 = 0;
    std::atomic_size_t aborted_f2 = 0;
    std::atomic_size_t execute_rejected = 0;
    std::atomic_size_t inactive_tx = 0;
    std::atomic<api::transaction_handle> tx{};
    std::size_t num_statements = 100;

    auto f1 = std::async(std::launch::async, [&]() {
        // repeat create tx, execute statement, destroy tx
        execute_n(
            [&](std::size_t i, callback_type cb) {
                db_->create_transaction_async([&, cb, i](transaction_handle t, status st, std::string_view msg) {
                    tx = t;
                    std::this_thread::sleep_for(10us);
                    auto ch0 = std::make_shared<test_channel>();
                    t.execute_async(
                        maybe_shared_ptr{stmt0.get()},
                        ch0,
                        [&, ch0, i, cb, t](status st, std::string_view msg) mutable {
                            ++statements_executed;
                            if (st != status::ok) {
                                if(st == status::err_invalid_argument) {
                                    ++execute_rejected;
                                } else if(st == status::err_inactive_transaction) {
                                    ++inactive_tx;
                                    // use EXPECT_ macros in lamda, otherwise gtest failed to detect failure
                                    std::shared_ptr<api::error_info> info{};
                                    EXPECT_EQ(status::ok, t.error_info(info));
                                    EXPECT_TRUE(info);
                                    EXPECT_EQ(status::err_unique_constraint_violation, info->status());
                                    EXPECT_EQ(error_code::unique_constraint_violation_exception, info->code());
                                } else {
                                    LOG(ERROR) << st;
                                }
                            } else {
                                t.commit();
                            }
                            if(db_->destroy_transaction(t) == status::ok) {
                                ++destroyed_f1;
                                transaction_handle tmp = t;
                                tx.compare_exchange_strong(tmp, transaction_handle{});
                            }
                            cb(i);
                        }
                    );
                });
            },
            [&]() {
                run0 = true;
            },
            num_statements
        );
    });
    auto f2 = std::async(std::launch::async, [&]() {
        while(! run0) {
            std::this_thread::sleep_for(2ms);
            if(auto t = static_cast<api::transaction_handle>(tx)) {
                // calling abort directly is not thread safe (abort will not work on sticky worker yet)
                t.execute_async(
                    maybe_shared_ptr{stmt1.get()},
                    [&](status st, std::string_view msg) {
                        if (st == status::err_unique_constraint_violation) {
                            ++aborted_f2;
                        } else if (st == status::err_invalid_argument || st == status::err_inactive_transaction) {
                            // tx already disposed or inactive - noop
                        } else if (st != status::ok) {
                            LOG(ERROR) << st;
                        }
                    }
                );
            }
        }
    });
    while(! run0.load()) {}
    // manually check most tx are aborted by f2, and f1 met some inactive tx
    std::cerr << "statements_executed:" << statements_executed << std::endl;
    std::cerr << "inactive_tx:" << inactive_tx << std::endl;
    std::cerr << "destroyed_f1:" << destroyed_f1 << std::endl;
    std::cerr << "aborted_f2:" << aborted_f2 << std::endl;
    std::cerr << "execute_rejected:" << execute_rejected << std::endl;
    ASSERT_LT(0, inactive_tx); // verify at least one inactive tx
    ASSERT_LT(0, aborted_f2); // verify at least one inactive tx
    ASSERT_EQ(0, get_impl(*db_).transaction_count());
}

auto get_termination_state(transaction_handle handle) {
    auto tctx = get_transaction_context(handle);
    return tctx->termination_mgr().state();
}

TEST_F(transaction_test, initial_termination_state) {
    auto tx = utils::create_transaction(*db_);
    {
        auto ts = get_termination_state(*tx);
        EXPECT_TRUE(! ts.going_to_commit());
        EXPECT_TRUE(! ts.going_to_abort());
        EXPECT_TRUE(ts.task_empty());
    }
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(transaction_test, commit_after_commit) {
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->commit());
    {
        auto ts = get_termination_state(*tx);
        EXPECT_TRUE(ts.going_to_commit());
        EXPECT_TRUE(! ts.going_to_abort());
        EXPECT_TRUE(ts.task_empty());
    }
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(transaction_test, commit_after_abort) {
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->abort_transaction());
    {
        auto ts = get_termination_state(*tx);
        EXPECT_TRUE(! ts.going_to_commit());
        EXPECT_TRUE(ts.going_to_abort());
        EXPECT_TRUE(ts.task_empty());
    }
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(transaction_test, abort_after_abort) {
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->abort_transaction());
    {
        auto ts = get_termination_state(*tx);
        EXPECT_TRUE(! ts.going_to_commit());
        EXPECT_TRUE(ts.going_to_abort());
        EXPECT_TRUE(ts.task_empty());
    }
    ASSERT_EQ(status::ok, tx->abort_transaction());
    {
        auto ts = get_termination_state(*tx);
        EXPECT_TRUE(! ts.going_to_commit());
        EXPECT_TRUE(ts.going_to_abort());
        EXPECT_TRUE(ts.task_empty());
    }
}

TEST_F(transaction_test, query_after_commit) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1)");
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->commit());
    test_stmt_err("select * from t", *tx, error_code::inactive_transaction_exception);
    test_stmt_err("insert into t values (2)", *tx, error_code::inactive_transaction_exception);
}

TEST_F(transaction_test, query_after_abort) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1)");
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->abort_transaction());
    test_stmt_err("select * from t", *tx, error_code::inactive_transaction_exception);
    test_stmt_err("insert into t values (2)", *tx, error_code::inactive_transaction_exception);
}

TEST_F(transaction_test, task_use_count) {
    // verify task_use_count is incremented and decremented correctly
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);

    auto rctx = impl::create_request_context(
        get_impl(*db_),
        tctx,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {},
        nullptr
    );

    std::atomic_bool executing = false;
    std::atomic_bool finish = false;
    auto task = create_custom_task(
        rctx.get(), [&]() {
            executing = true;
            while (! finish) {}
        return model::task_result::complete;
    },
        model::task_transaction_kind::in_transaction);
    EXPECT_TRUE(! task.sticky());
    EXPECT_TRUE(task.in_transaction());

    std::atomic_bool executed = false;
    tateyama::task_scheduler::context ctx{};
    auto f = std::async(std::launch::async, [&]() {
        task.execute(ctx);
        executed = true;
    });
    while (! executing) {}
    ASSERT_EQ(1, tctx->termination_mgr().state().task_use_count());
    finish = true;
    while (! executed) {}
    ASSERT_EQ(0, tctx->termination_mgr().state().task_use_count());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(transaction_test, commit_while_task_is_running) {
    // verify commit aborts when there are on-going tasks
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);

    auto rctx = impl::create_request_context(
        get_impl(*db_),
        tctx,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {},
        nullptr
    );

    std::atomic_bool executing = false;
    std::atomic_bool finish = false;
    auto task = create_custom_task(
        rctx.get(), [&]() {
            executing = true;
            while (! finish) {}
        return model::task_result::complete;
    },
        model::task_transaction_kind::in_transaction);
    EXPECT_TRUE(! task.sticky());
    EXPECT_TRUE(task.in_transaction());

    std::atomic_bool executed = false;
    tateyama::task_scheduler::context ctx{};
    auto f = std::async(std::launch::async, [&]() {
        task.execute(ctx);
        executed = true;
    });
    while (! executing) {}
    ASSERT_EQ(1, tctx->termination_mgr().state().task_use_count());

    ASSERT_EQ(status::err_illegal_operation, tx->commit());
    ASSERT_TRUE(tctx->termination_mgr().state().going_to_abort());
    ASSERT_TRUE(! tctx->termination_mgr().state().going_to_commit());

    EXPECT_EQ(transaction_state_kind::going_to_abort, tctx->state());

    finish = true;
    while (! executed) {}
    ASSERT_EQ(0, tctx->termination_mgr().state().task_use_count());
    EXPECT_EQ(transaction_state_kind::aborted, tctx->state());

    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(transaction_test, abort_while_task_is_running) {
    // verify commit aborts when there are on-going tasks
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);

    auto rctx = impl::create_request_context(
        get_impl(*db_),
        tctx,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {},
        nullptr
    );

    std::atomic_bool executing = false;
    std::atomic_bool finish = false;
    auto task = create_custom_task(
        rctx.get(), [&]() {
            executing = true;
            while (! finish) {}
        return model::task_result::complete;
    },
        model::task_transaction_kind::in_transaction);
    EXPECT_TRUE(! task.sticky());
    EXPECT_TRUE(task.in_transaction());

    std::atomic_bool executed = false;
    tateyama::task_scheduler::context ctx{};
    auto f = std::async(std::launch::async, [&]() {
        task.execute(ctx);
        executed = true;
    });
    while (! executing) {}
    ASSERT_EQ(1, tctx->termination_mgr().state().task_use_count());

    ASSERT_EQ(status::ok, tx->abort_transaction());
    ASSERT_TRUE(tctx->termination_mgr().state().going_to_abort());
    ASSERT_TRUE(! tctx->termination_mgr().state().going_to_commit());

    EXPECT_EQ(transaction_state_kind::going_to_abort, tctx->state());

    finish = true;
    while (! executed) {}
    ASSERT_EQ(0, tctx->termination_mgr().state().task_use_count());

    EXPECT_EQ(transaction_state_kind::aborted, tctx->state());

    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}


}

