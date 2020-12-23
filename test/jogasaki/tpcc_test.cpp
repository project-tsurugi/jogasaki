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

#include <regex>
#include <gtest/gtest.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/database_impl.h>
#include <jogasaki/api/result_set.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

class tpcc_test : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        db_.start();
        auto db_impl = api::database::impl::get_impl(db_);
        add_benchmark_tables(*db_impl->tables());
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "WAREHOUSE0", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "DISTRICT0", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "CUSTOMER0", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "CUSTOMER1", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "NEW_ORDER0", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ORDERS0", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ORDERS1", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ORDER_LINE0", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ITEM0", 10, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "STOCK0", 10, true, 5);
    }
    static void TearDownTestSuite() {
        db_.stop();
    }

    void execute_query(std::string_view query) {
        auto rs = db_.query(query);
        auto it = rs->begin();
        while(it != rs->end()) {
            auto record = it.ref();
            std::stringstream ss{};
            ss << record << *rs->meta();
            LOG(INFO) << ss.str();
            ++it;
        }
        rs->close();
    }
    void execute_statement(std::string_view query) {
        (void)db_.query(query);
    }

    static jogasaki::api::database db_;
};

jogasaki::api::database tpcc_test::db_{};

TEST_F(tpcc_test, warehouse) {
    auto rs = db_.query("SELECT * FROM WAREHOUSE");
    auto it = rs->begin();
    while(it != rs->end()) {
        auto record = it.ref();
        std::stringstream ss{};
        ss << record << *rs->meta();
        LOG(INFO) << ss.str();
        ++it;
    }
    rs->close();
}

void resolve(std::string& query, std::string_view place_holder, std::string value) {
    query = std::regex_replace(query, std::regex(std::string(place_holder)), value);
}

TEST_F(tpcc_test, new_order1) {
    std::string query =
        "SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER "
        "WHERE w_id = :w_id "
        "AND c_w_id = w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id "
        ;

    resolve(query, ":w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, new_order2) {
    std::string query =
        "SELECT d_next_o_id, d_tax FROM DISTRICT "
        "WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id "
    ;

    resolve(query, ":d_w_id", "1");
    resolve(query, ":d_id", "1");
    execute_query(query);
}
TEST_F(tpcc_test, new_order_update) {
    std::string query =
        "UPDATE "
        "DISTRICT SET "
        "d_next_o_id = :d_next_o_id WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id"
    ;

    resolve(query, ":d_next_o_id", "2");
    resolve(query, ":d_w_id", "1");
    resolve(query, ":d_id", "1");
    execute_statement(query);
}

}
