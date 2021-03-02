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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class tpcc_test : public ::testing::Test {
public:
    // change this flag to debug with explain
    constexpr static bool to_explain = false;

    void SetUp() {
        auto cfg = std::make_shared<configuration>();
        db_ = api::create_database(cfg);
        db_->start();
        auto* db_impl = unsafe_downcast<api::impl::database>(db_.get());
        add_benchmark_tables(*db_impl->tables());
        register_kvs_storage(*db_impl->kvs_db(), *db_impl->tables());
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "WAREHOUSE0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "DISTRICT0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "CUSTOMER0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "CUSTOMER_SECONDARY0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "NEW_ORDER0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ORDERS0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ORDERS_SECONDARY0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ORDER_LINE0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "ITEM0", 3, true, 5);
        utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "STOCK0", 3, true, 5);
    }

    void TearDown() {
        db_->stop();
    }

    void explain(api::executable_statement& stmt) {
        if (to_explain) {
            db_->explain(stmt, std::cout);
            std::cout << std::endl;
        }
    }
    void execute_query(std::string_view query, std::vector<mock::basic_record>& out) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(query, stmt));
        explain(*stmt);
        auto tx = db_->create_transaction();
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok, tx->execute(*stmt, rs));
        ASSERT_TRUE(rs);
        auto it = rs->iterator();
        while(it->has_next()) {
            auto* record = it->next();
            std::stringstream ss{};
            ss << *record;
            auto* rec_impl = unsafe_downcast<api::impl::record>(record);
            auto* meta_impl = unsafe_downcast<api::impl::record_meta>(rs->meta());
            out.emplace_back(rec_impl->ref(), meta_impl->meta());
            LOG(INFO) << ss.str();
        }
        rs->close();
        tx->commit();
    }

    void execute_statement(std::string_view query) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(query, stmt));
        explain(*stmt);
        auto tx = db_->create_transaction();
        ASSERT_EQ(status::ok, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->commit());
    }

    std::unique_ptr<jogasaki::api::database> db_;
};

using namespace std::string_view_literals;

TEST_F(tpcc_test, warehouse) {
    execute_statement( "INSERT INTO WAREHOUSE (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (10, 'fogereb', 'byqosjahzgrvmmmpglb', 'kezsiaxnywrh', 'jisagjxblbmp', 'ps', '694764299', 0.12, 3000000.00)");
    execute_statement( "INSERT INTO WAREHOUSE (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (20, 'fogereb', 'byqosjahzgrvmmmpglb', 'kezsiaxnywrh', 'jisagjxblbmp', 'ps', '694764299', 0.12, 3000000.00)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM WAREHOUSE WHERE w_id >= 10 ORDER BY w_id", result);
    ASSERT_EQ(2, result.size());
    auto& rec = result[0];
    EXPECT_EQ(10, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    EXPECT_EQ(accessor::text("fogereb"), rec.ref().get_value<accessor::text>(rec.record_meta()->value_offset(1)));

    EXPECT_EQ(20, result[1].ref().get_value<std::int64_t>(result[1].record_meta()->value_offset(0)));
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

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(1.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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

    std::string verify =
        "SELECT d_next_o_id FROM DISTRICT "
        "WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id "
    ;
    resolve(verify, ":d_w_id", "1");
    resolve(verify, ":d_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(2, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, new_order_insert1) {
    std::string query =
        "INSERT INTO "
        "ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) "
        "VALUES (:o_id, :o_d_id, :o_w_id, :o_c_id, :o_entry_d, :o_ol_cnt, :o_all_local)"
    ;

    resolve(query, ":o_id", "10");
    resolve(query, ":o_d_id", "10");
    resolve(query, ":o_w_id", "10");
    resolve(query, ":o_c_id", "10");
    resolve(query, ":o_entry_d", "'X'");
    resolve(query, ":o_ol_cnt", "10");
    resolve(query, ":o_all_local", "10");
    execute_statement(query);

    std::string verify =
        "SELECT o_c_id FROM ORDERS "
        "WHERE "
        "o_id = :o_id AND "
        "o_d_id = :o_d_id AND "
        "o_w_id = :o_w_id"
    ;
    resolve(verify, ":o_id", "10");
    resolve(verify, ":o_d_id", "10");
    resolve(verify, ":o_w_id", "10");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(10, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, new_order_insert2) {
    std::string query =
        "INSERT INTO "
        "ORDERS_SECONDARY (o_d_id, o_w_id, o_c_id, o_id) "
        "VALUES (:o_d_id, :o_w_id, :o_c_id, :o_id)"
    ;

    resolve(query, ":o_d_id", "10");
    resolve(query, ":o_w_id", "10");
    resolve(query, ":o_c_id", "10");
    resolve(query, ":o_id", "10");
    execute_statement(query);

    std::string verify =
        "SELECT o_id FROM ORDERS_SECONDARY "
        "WHERE "
        "o_d_id = :o_d_id AND "
        "o_w_id = :o_w_id AND "
        "o_c_id = :o_c_id "
    ;
    resolve(verify, ":o_d_id", "10");
    resolve(verify, ":o_w_id", "10");
    resolve(verify, ":o_c_id", "10");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(10, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, new_order_insert3) {
    std::string query =
        "INSERT INTO "
        "NEW_ORDER (no_o_id, no_d_id, no_w_id)"
        "VALUES (:no_o_id, :no_d_id, :no_w_id)"
    ;

    resolve(query, ":no_o_id", "10");
    resolve(query, ":no_d_id", "10");
    resolve(query, ":no_w_id", "10");
    execute_statement(query);

    std::string verify =
        "SELECT no_o_id FROM NEW_ORDER "
        "WHERE "
        "no_o_id = :no_o_id AND "
        "no_d_id = :no_d_id AND "
        "no_w_id = :no_w_id "
    ;
    resolve(verify, ":no_o_id", "10");
    resolve(verify, ":no_d_id", "10");
    resolve(verify, ":no_w_id", "10");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(10, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, new_order3) {
    std::string query =
        "SELECT i_price, i_name , i_data FROM ITEM "
        "WHERE "
        "i_id = :i_id"
    ;

    resolve(query, ":i_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(1.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, new_order4) {
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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

    std::string verify =
        "SELECT s_quantity FROM STOCK "
        "WHERE "
        "s_i_id = :s_i_id AND "
        "s_w_id = :s_w_id"
    ;
    resolve(verify, ":s_i_id", "1");
    resolve(verify, ":s_w_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(2, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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

    std::string verify =
        "SELECT w_ytd FROM WAREHOUSE "
        "WHERE "
        "w_id = :w_id"
    ;
    resolve(verify, ":w_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(101.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
}


TEST_F(tpcc_test, payment1) {
    std::string query =
        "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM WAREHOUSE "
        "WHERE "
        "w_id = :w_id"
    ;

    resolve(query, ":w_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(accessor::text("BBBBBBBBBBBBBBBBBBBBBB"), result[0].ref().get_value<accessor::text>(result[0].record_meta()->value_offset(0)));
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

    std::string verify =
        "SELECT d_ytd FROM DISTRICT "
        "WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id "
    ;
    resolve(verify, ":d_w_id", "1");
    resolve(verify, ":d_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(101.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(accessor::text("BBBBBBBBBBBBBBBBBBBBBB"), result[0].ref().get_value<accessor::text>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, payment3) {
    std::string query =
        "SELECT COUNT(c_id) FROM CUSTOMER_SECONDARY "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last"
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, payment4) {
    std::string query =
        "SELECT c_id FROM CUSTOMER_SECONDARY "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last "
        " ORDER by c_first "
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(accessor::text("BBBBBBBBBBBBBBBBBBBBBB"), result[0].ref().get_value<accessor::text>(result[0].record_meta()->value_offset(0)));
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(accessor::text("BBBBBBBBBBBBBBBBBBBBBB"), result[0].ref().get_value<accessor::text>(result[0].record_meta()->value_offset(0)));
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

    resolve(query, ":c_balance", "2.0");
    resolve(query, ":c_data", "'XX'");
    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_id", "1");
    execute_statement(query);

    std::string verify =
        "SELECT c_balance, c_data FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id"
    ;
    resolve(verify, ":c_w_id", "1");
    resolve(verify, ":c_d_id", "1");
    resolve(verify, ":c_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(2.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
    EXPECT_EQ(accessor::text("XX"), result[0].ref().get_value<accessor::text>(result[0].record_meta()->value_offset(1)));
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

    resolve(query, ":c_balance", "10.0");
    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_id", "1");
    execute_statement(query);

    std::string verify =
        "SELECT c_balance FROM CUSTOMER "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id"
    ;
    resolve(verify, ":c_w_id", "1");
    resolve(verify, ":c_d_id", "1");
    resolve(verify, ":c_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(10.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, order_status1) {
    std::string query =
        "SELECT COUNT(c_id) FROM CUSTOMER_SECONDARY "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last"
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
}

TEST_F(tpcc_test, order_status2) {
    std::string query =
        "SELECT c_id FROM CUSTOMER_SECONDARY "
        "WHERE "
        "c_w_id = :c_w_id AND "
        "c_d_id = :c_d_id AND "
        "c_last = :c_last "
        " ORDER by c_first "
    ;

    resolve(query, ":c_w_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_last", "'BBBBBBBBBBBBBBBBBBBBBB'");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(1.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, order_status4) {
    std::string query =
        "SELECT o_id FROM ORDERS_SECONDARY "
        "WHERE "
        "o_w_id = :o_w_id AND "
        "o_d_id = :o_d_id AND "
        "o_c_id = :o_c_id"
        " ORDER by o_id DESC"
    ;

    resolve(query, ":o_w_id", "1");
    resolve(query, ":o_d_id", "1");
    resolve(query, ":o_c_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, order_status5) {
    std::string query =
        "SELECT o_carrier_id, o_entry_d, o_ol_cnt "
        "FROM ORDERS "
        "WHERE o_w_id = :o_w_id AND "
        "o_d_id = :o_d_id AND "
        "o_id = :o_id"
    ;

    resolve(query, ":o_w_id", "1");
    resolve(query, ":o_d_id", "1");
    resolve(query, ":o_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, order_status6) {
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, delivery1) {
    std::string query =
        "SELECT no_o_id FROM NEW_ORDER "
        "WHERE "
        "no_d_id = :no_d_id AND "
        "no_w_id = :no_w_id "
        "ORDER BY no_o_id "
    ;

    resolve(query, ":no_d_id", "1");
    resolve(query, ":no_w_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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

    std::string verify =
        "SELECT no_o_id FROM NEW_ORDER "
        "WHERE "
        "no_d_id = :no_d_id AND "
        "no_w_id = :no_w_id AND "
        "no_o_id = :no_o_id"
    ;
    resolve(verify, ":no_d_id", "1");
    resolve(verify, ":no_w_id", "1");
    resolve(verify, ":no_o_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(0, result.size());
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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

    resolve(query, ":o_carrier_id", "10"); // nullable
    resolve(query, ":o_id", "1");
    resolve(query, ":o_d_id", "1");
    resolve(query, ":o_w_id", "1");
    execute_statement(query);

    std::string verify =
        "SELECT o_carrier_id FROM ORDERS "
        "WHERE "
        "o_id = :o_id AND "
        "o_d_id = :o_d_id AND "
        "o_w_id = :o_w_id"
        ;
    resolve(verify, ":o_id", "1");
    resolve(verify, ":o_d_id", "1");
    resolve(verify, ":o_w_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(10, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
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

    std::string verify =
        "SELECT ol_delivery_d FROM ORDER_LINE "
        "WHERE "
        "ol_o_id = :ol_o_id AND "
        "ol_d_id = :ol_d_id AND "
        "ol_w_id = :ol_w_id"
    ;
    resolve(verify, ":ol_o_id", "1");
    resolve(verify, ":ol_d_id", "1");
    resolve(verify, ":ol_w_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(accessor::text("A"), result[0].ref().get_value<accessor::text>(result[0].record_meta()->value_offset(0)));
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(1.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
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

    resolve(query, ":ol_total", "100.0"); // nullable
    resolve(query, ":c_id", "1");
    resolve(query, ":c_d_id", "1");
    resolve(query, ":c_w_id", "1");
    execute_statement(query);

    std::string verify =
        "SELECT c_balance FROM CUSTOMER "
        "WHERE "
        "c_id = :c_id AND "
        "c_d_id = :c_d_id AND "
        "c_w_id = :c_w_id"
    ;
    resolve(verify, ":c_id", "1");
    resolve(verify, ":c_d_id", "1");
    resolve(verify, ":c_w_id", "1");
    std::vector<mock::basic_record> result{};
    execute_query(verify, result);
    ASSERT_EQ(1, result.size());
    EXPECT_DOUBLE_EQ(101.0, result[0].ref().get_value<double>(result[0].record_meta()->value_offset(0)));
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
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

TEST_F(tpcc_test, stock_level2) {
    std::string query =
//        "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id "
        "SELECT COUNT(s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id "
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
    resolve(query, ":s_quantity", "10");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(result[0].record_meta()->value_offset(0)));
}

}
