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
#include <jogasaki/api.h>

#include <thread>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/mock/basic_record.h>
#include "api_test_base.h"
#include <jogasaki/utils/msgbuf_utils.h>

namespace jogasaki::api {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using impl::get_impl;

/**
 * @brief test database api
 */
class transaction_impl_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(transaction_impl_test, resolve_execute_stmt) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(:p0, :p1)", variables, prepared));

    auto transaction = utils::create_transaction(*db_);
    auto tx = get_impl(*db_).find_transaction(*transaction);

    auto ps = api::create_parameter_set();
    ps->set_int8("p0", 1);
    ps->set_float8("p1", 10.0);

    std::atomic_bool run0{false}, error_abort{false};
    ASSERT_TRUE(executor::execute_async(
        get_impl(*db_),
        tx,
        prepared,
        std::shared_ptr{std::move(ps)},
        nullptr,
        [&](status st, std::string_view msg){
            if(st != status::ok) {
                LOG(ERROR) << st;
                error_abort.store(true);
            }
            run0.store(true);
        }
    ));
    while(! error_abort.load() && !run0.load()) {}
    ASSERT_EQ(status::ok, executor::commit(get_impl(*db_), tx));
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T0", result);
        ASSERT_EQ(1, result.size());
        ASSERT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(1, 10.0)), result[0]);
    }
}

}
