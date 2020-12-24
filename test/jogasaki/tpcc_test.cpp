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

TEST_F(tpcc_test, new_order_update1) {
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

TEST_F(tpcc_test, new_order3) {
    std::string query =
        "SELECT s_quantity, s_data, "
        "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
        "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM STOCK "
        "WHERE "
        "s_i_id = :s_i_id AND "
        "s_w_id = :s_w_id"
    ;

    resolve(query, ":s_i_id", "1");
    resolve(query, ":s_w_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, new_order_update2) {
    std::string query =
        "UPDATE "
        "STOCK SET "
        "s_quantity = :s_quantity WHERE "
        "s_i_id = :s_i_id AND "
        "s_w_id = :s_w_id"
    ;

    resolve(query, ":s_quantity", "2");
    resolve(query, ":s_i_id", "1");
    resolve(query, ":s_w_id", "1");
    execute_statement(query);
}

TEST_F(tpcc_test, payment_update1) {
    std::string query =
        "UPDATE "
        "WAREHOUSE SET "
        "w_ytd = w_ytd + :h_amount WHERE "
        "w_id = :w_id"
    ;

    resolve(query, ":h_amount", "100.0");
    resolve(query, ":w_id", "1");
    execute_statement(query);
}


TEST_F(tpcc_test, payment1) {
    std::string query =
        "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM WAREHOUSE "
        "WHERE "
        "w_id = :w_id"
    ;

    resolve(query, ":w_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, payment_update2) {
    std::string query =
        "UPDATE "
        "DISTRICT SET "
        "d_ytd = d_ytd + :h_amount WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id"
    ;

    resolve(query, ":h_amount", "100.0");
    resolve(query, ":d_w_id", "1");
    resolve(query, ":d_id", "1");
    execute_statement(query);
}

TEST_F(tpcc_test, payment2) {
    std::string query =
        "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM DISTRICT "
        "WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id"
    ;

    resolve(query, ":d_w_id", "1");
    resolve(query, ":d_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, payment3) {
    std::string query =
        "SELECT COUNT(c_id) FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last"
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    execute_query(query);
}

TEST_F(tpcc_test, payment4) {
    std::string query =
        "SELECT c_id FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last "
        " ORDER by c_first "
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    execute_query(query);
}

TEST_F(tpcc_test, payment5) {
    std::string query =
        "SELECT c_first, c_middle, c_last, "
        "c_street_1, c_street_2, c_city, c_state, c_zip, "
        "c_phone, c_credit, c_credit_lim, "
        "c_discount, c_balance, c_since FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id"
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, payment6) {
    std::string query =
        "SELECT c_data FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id"
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, payment_update3) {
    std::string query =
        "UPDATE "
        "CUSTOMER SET "
        "c_balance = :c_balance ,"
        "c_data = :c_data WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id"
    ;

    resolve(query, ":c_balance", "1.0");
    resolve(query, ":c_data", "'A'");
    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_id", "1");
    execute_statement(query);
}

TEST_F(tpcc_test, payment_update4) {
    std::string query =
        "UPDATE "
        "CUSTOMER SET "
        "c_balance = :c_balance WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id"
    ;

    resolve(query, ":c_balance", "1.0");
    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_id", "1");
    execute_statement(query);
}

TEST_F(tpcc_test, order_status1) {
    std::string query =
        "SELECT COUNT(c_id) FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last"
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    execute_query(query);
}

TEST_F(tpcc_test, order_status2) {
    std::string query =
        "SELECT c_id FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last "
        " ORDER by c_first "
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    execute_query(query);
}

TEST_F(tpcc_test, order_status3) {
    std::string query =
        "SELECT c_balance, c_first, c_middle, c_last FROM CUSTOMER "
        "WHERE "
        "c_id = :c_id AND "
        "c_d_id = :c_d_id AND "
        "c_w_id = :c_w_id"
    ;

    resolve(query, ":c_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_w_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, order_status4) {
    std::string query =
        "SELECT o_id FROM ORDERS "
        "WHERE "
        "o_d_id = :o_d_id AND "
        "o_w_id = :o_w_id AND "
        "o_c_id = :o_c_id"
        " ORDER by o_id DESC"
    ;

    resolve(query, ":o_d_id", "1");
    resolve(query, ":o_w_id", "1");
    resolve(query, ":o_c_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, order_status5) {
    std::string query =
        "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM ORDER_LINE "
        "WHERE "
        "ol_o_id = :ol_o_id AND "
        "ol_d_id = :ol_d_id AND "
        "ol_w_id = :ol_w_id"
    ;

    resolve(query, ":ol_o_id", "1");
    resolve(query, ":ol_d_id", "1");
    resolve(query, ":ol_w_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, delivery1) {
    std::string query =
        "SELECT no_o_id FROM NEW_ORDER "
        "WHERE "
        "no_d_id = :no_d_id AND "
        "no_w_id = :no_w_id"
    ;

    resolve(query, ":no_d_id", "1");
    resolve(query, ":no_w_id", "1");
    execute_query(query);
}
TEST_F(tpcc_test, delivery_delete1) {
    std::string query =
        "DELETE FROM NEW_ORDER "
        "WHERE "
        "no_d_id = :no_d_id AND "
        "no_w_id = :no_w_id AND "
        "no_o_id = :no_o_id"
    ;

    resolve(query, ":no_d_id", "1");
    resolve(query, ":no_w_id", "1");
    resolve(query, ":no_o_id", "1");
    execute_statement(query);
}
TEST_F(tpcc_test, delivery2) {
    std::string query =
        "SELECT o_c_id FROM ORDERS "
        "WHERE "
        "o_id = :o_id AND "
        "o_d_id = :o_d_id AND "
        "o_w_id = :o_w_id"
    ;

    resolve(query, ":o_id", "1");
    resolve(query, ":o_d_id", "1");
    resolve(query, ":o_w_id", "1");
    execute_query(query);
}
TEST_F(tpcc_test, delivery_update1) {
    std::string query =
        "UPDATE "
        "ORDERS SET "
        "o_carrier_id = :o_carrier_id WHERE "
        "o_id = :o_id AND "
        "o_d_id = :o_d_id AND "
        "o_w_id = :o_w_id"
    ;

    resolve(query, ":o_carrier_id", "1"); // nullable
    resolve(query, ":o_id", "1");
    resolve(query, ":o_d_id", "1");
    resolve(query, ":o_w_id", "1");
    execute_statement(query);
}

TEST_F(tpcc_test, delivery_update2) {
    std::string query =
        "UPDATE "
        "ORDER_LINE SET "
        "ol_delivery_d = :ol_delivery_d WHERE "
        "ol_o_id = :ol_o_id AND "
        "ol_d_id = :ol_d_id AND "
        "ol_w_id = :ol_w_id"
    ;

    resolve(query, ":ol_delivery_d", "'A'"); // nullable
    resolve(query, ":ol_o_id", "1");
    resolve(query, ":ol_d_id", "1");
    resolve(query, ":ol_w_id", "1");
    execute_statement(query);
}

TEST_F(tpcc_test, delivery3) {
    std::string query =
        "SELECT SUM(ol_amount) FROM ORDER_LINE WHERE "
        "ol_o_id = :ol_o_id AND "
        "ol_d_id = :ol_d_id AND "
        "ol_w_id = :ol_w_id"
    ;

    resolve(query, ":ol_o_id", "1");
    resolve(query, ":ol_d_id", "1");
    resolve(query, ":ol_w_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, delivery_update3) {
    std::string query =
        "UPDATE "
        "CUSTOMER SET "
        "c_balance = c_balance + :ol_total WHERE "
        "c_id = :c_id AND "
        "c_d_id = :c_d_id AND "
        "c_w_id = :c_w_id"
    ;

    resolve(query, ":ol_total", "1.0"); // nullable
    resolve(query, ":c_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_w_id", "1");
    execute_statement(query);
}

TEST_F(tpcc_test, stock_level1) {
    std::string query =
        "SELECT d_next_o_id FROM DISTRICT "
        "WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id"
    ;

    resolve(query, ":d_w_id", "1");
    resolve(query, ":d_id", "1");
    execute_query(query);
}

TEST_F(tpcc_test, stock_level2) {
    std::string query =
        "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id "
        "WHERE "
        "ol_w_id = :ol_w_id AND "
        "ol_d_id = :ol_d_id AND "
        "ol_o_id < :ol_o_id_high AND "
        "ol_o_id >= :ol_o_id_low AND "
        "s_w_id = :s_w_id AND "
        "s_quantity < :s_quantity"
    ;

    resolve(query, ":ol_w_id", "1");
    resolve(query, ":ol_d_id", "1");
    resolve(query, ":ol_o_id_high", "10");
    resolve(query, ":ol_o_id_low", "1");
    resolve(query, ":s_w_id", "1");
    resolve(query, ":s_quantity", "10.0");
    execute_query(query);
}

}
