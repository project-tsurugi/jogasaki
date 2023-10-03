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
#include <jogasaki/api.h>

#include <thread>
#include <atomic>
#include <future>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/mock/test_channel.h>
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
class many_transactions_test :
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

TEST_F(many_transactions_test, create_many_tx) {
    if(kvs::implementation_id() == "memory") {
        // sharksfin-memory has no specific limit for the number of tx
        GTEST_SKIP();
    }
    constexpr std::size_t num_transactions = 1000;
    bool limit_reached = false;
    std::vector<api::transaction_handle> txs(num_transactions);
    transaction_option option{};
    std::size_t count = 0;
    for(std::size_t i=0; i < num_transactions; ++i, ++count) {
        std::shared_ptr<api::error_info> error{};
        if(auto rc = get_impl(*db_).do_create_transaction(txs[i], option, error); rc != status::ok) {
            ASSERT_TRUE(error);
            std::cerr << *error << std::endl;
            ASSERT_EQ(error_code::transaction_exceeded_limit_exception, error->code());
            limit_reached = true;
            break;
        }
    }
    for(std::size_t i=0; i < count; ++i) {
        ASSERT_EQ(status::ok, txs[i].commit());
    }
    for(std::size_t i=0; i < count; ++i) {
        ASSERT_EQ(status::ok, get_impl(*db_).destroy_transaction(txs[i]));
    }
    ASSERT_TRUE(limit_reached);
}


}

