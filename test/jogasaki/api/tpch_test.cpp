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

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;
using takatori::util::fail;
using accessor::text;

class tpch_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_ = api::create_database(cfg);
        db_->start();
        auto* db_impl = unsafe_downcast<api::impl::database>(db_.get());
        add_analytics_benchmark_tables(*db_impl->tables());
        register_kvs_storage(*db_impl->kvs_db(), *db_impl->tables());
        utils::load_storage_data(*db_, db_impl->tables(), "PART", 3, true, 5);
        utils::load_storage_data(*db_, db_impl->tables(), "SUPPLIER", 3, true, 5);
        utils::load_storage_data(*db_, db_impl->tables(), "PARTSUPP", 3, true, 5);
        utils::load_storage_data(*db_, db_impl->tables(), "CUSTOMER", 3, true, 5);
        utils::load_storage_data(*db_, db_impl->tables(), "ORDERS", 3, true, 5);
        utils::load_storage_data(*db_, db_impl->tables(), "LINEITEM", 3, true, 5);
        utils::load_storage_data(*db_, db_impl->tables(), "NATION", 3, true, 5);
        utils::load_storage_data(*db_, db_impl->tables(), "REGION", 3, true, 5);
    }

    void TearDown() override {
        db_->stop();
    }

    template <class T>
    void set(api::parameter_set& ps, std::string_view place_holder, api::field_type_kind kind, T value) {
        db_->register_variable(place_holder, kind);
        switch(kind) {
            case api::field_type_kind::int4:
                if constexpr (std::is_convertible_v<T, std::int32_t>) {
                    ps.set_int4(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::int8:
                if constexpr (std::is_convertible_v<T, std::int64_t>) {
                    ps.set_int8(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::float4:
                if constexpr (std::is_convertible_v<T, float>) {
                    ps.set_float4(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::float8:
                if constexpr (std::is_convertible_v<T, double>) {
                    ps.set_float8(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::character:
                if constexpr (std::is_convertible_v<T, std::string_view>) {
                    ps.set_character(place_holder, value);
                } else {
                    fail();
                }
                break;
            default:
                fail();
        }
    }
};

using namespace std::string_view_literals;

TEST_F(tpch_test, q2_1) {
    std::string query =
        "SELECT MIN(PS_SUPPLYCOST) "
        "FROM PARTSUPP, SUPPLIER, NATION, REGION "
        "WHERE "
        "PS_SUPPKEY = S_SUPPKEY "
        "AND S_NATIONKEY = N_NATIONKEY "
        "AND N_REGIONKEY = R_REGIONKEY "
        "AND R_NAME = :region "
        "AND PS_PARTKEY = :partkey ";

    auto ps = api::create_parameter_set();
    set(*ps, "region", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "partkey", api::field_type_kind::int8, 1);

    std::vector<mock::basic_record> result{};
    execute_query(query, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
}

TEST_F(tpch_test, q2_2) {
    std::string query =
        "SELECT S_ACCTBAL, S_NAME, N_NAME, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT "
        "FROM PART, SUPPLIER, PARTSUPP, NATION, REGION "
        "WHERE "
        "S_SUPPKEY = PS_SUPPKEY "
        "AND S_NATIONKEY = N_NATIONKEY "
        "AND N_REGIONKEY = R_REGIONKEY "
        "AND PS_PARTKEY = :partkey "
        "AND P_SIZE = :size "
        "AND P_TYPE3 = :type "
        "AND R_NAME = :region "
        "AND PS_SUPPLYCOST = :mincost "
        "ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY";

    auto ps = api::create_parameter_set();
    set(*ps, "partkey", api::field_type_kind::int8, 1);
    set(*ps, "size", api::field_type_kind::int8, 1);
    set(*ps, "type", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "region", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "mincost", api::field_type_kind::int8, 1);

    std::vector<mock::basic_record> result{};
    execute_query(query, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
}

TEST_F(tpch_test, q6) {
    std::string query =
        "SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE "
        "FROM LINEITEM "
        "WHERE "
        "L_SHIPDATE >= :datefrom "
        "AND L_SHIPDATE < :dateto "
        "AND L_DISCOUNT >= :discount - 1 "
        "AND L_DISCOUNT <= :discount + 1 "
        "AND L_QUANTITY < :quantity";

    auto ps = api::create_parameter_set();
    set(*ps, "datefrom", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "dateto", api::field_type_kind::character, "CCCCCCCCCCCCCCCCCCCCCC"sv);
    set(*ps, "discount", api::field_type_kind::int8, 1);
    set(*ps, "quantity", api::field_type_kind::int8, 2);

    std::vector<mock::basic_record> result{};
    execute_query(query, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
}

TEST_F(tpch_test, q14m) {
    std::string query =
        "SELECT "
        "SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS MOLECULE "
        "FROM LINEITEM, PART "
        "WHERE "
        "L_PARTKEY = P_PARTKEY "
        "AND P_TYPE1 = 'BBBBBBBBBBBBBBBBBBBBBB' "
        "AND L_SHIPDATE >= :datefrom "
        "AND L_SHIPDATE < :dateto";

    auto ps = api::create_parameter_set();
    set(*ps, "datefrom", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "dateto", api::field_type_kind::character, "CCCCCCCCCCCCCCCCCCCCCC"sv);

    std::vector<mock::basic_record> result{};
    execute_query(query, *ps, result);
    ASSERT_EQ(1, result.size());
}

TEST_F(tpch_test, q14d) {
    std::string query =
        "SELECT "
        "SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS DENOMINATOR "
        "FROM LINEITEM, PART "
        "WHERE "
        "L_PARTKEY = P_PARTKEY "
        "AND L_SHIPDATE >= :datefrom "
        "AND L_SHIPDATE < :dateto";

    auto ps = api::create_parameter_set();
    set(*ps, "datefrom", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "dateto", api::field_type_kind::character, "CCCCCCCCCCCCCCCCCCCCCC"sv);

    std::vector<mock::basic_record> result{};
    execute_query(query, *ps, result);
    ASSERT_EQ(1, result.size());
}

TEST_F(tpch_test, q19) {
    std::string query =
        "SELECT SUM(L_EXTENDEDPRICE * (100 - L_DISCOUNT)) AS REVENUE "
        "FROM LINEITEM, PART "
        "WHERE "
        "P_PARTKEY = L_PARTKEY "
        "AND (( "
        "P_BRAND = :brand1 "
//        "AND ( P_CONTAINER = 'SM CASE   ' OR  P_CONTAINER = 'SM BOX    ' OR P_CONTAINER = 'SM PACK   ' OR P_CONTAINER = 'SM PKG    ' ) "
        "AND ( P_CONTAINER = 'BBBBBBBBBBBBBBBBBBBBBB' OR  P_CONTAINER = 'SM BOX    ' OR P_CONTAINER = 'SM PACK   ' OR P_CONTAINER = 'SM PKG    ' ) "
        "AND L_QUANTITY >= :quantity1 AND L_QUANTITY <= :quantity1 + 10 "
        "AND P_SIZE >= 1 AND P_SIZE <= 5 "
//        "AND ( L_SHIPMODE = 'AIR       ' OR  L_SHIPMODE = 'AIR REG   ' ) "
        "AND ( L_SHIPMODE = 'BBBBBBBBBBBBBBBBBBBBBB' OR  L_SHIPMODE = 'AIR REG   ' ) "
//        "AND L_SHIPINSTRUCT = 'DELIVER IN PERSON        ' "
        "AND L_SHIPINSTRUCT = 'BBBBBBBBBBBBBBBBBBBBBB' "
        ") OR ( "
        "P_BRAND = :brand2 "
//        "AND ( P_CONTAINER = 'MED BAG   ' OR  P_CONTAINER = 'MED BOX   ' OR P_CONTAINER = 'MED PKG   ' OR P_CONTAINER = 'MED PACK  ' ) "
        "AND ( P_CONTAINER = 'BBBBBBBBBBBBBBBBBBBBBB' OR  P_CONTAINER = 'MED BOX   ' OR P_CONTAINER = 'MED PKG   ' OR P_CONTAINER = 'MED PACK  ' ) "
        "AND L_QUANTITY >= :quantity2 AND L_QUANTITY <= :quantity2 + 10 "
        "AND P_SIZE >= 1 AND P_SIZE <= 10 "
//        "AND ( L_SHIPMODE = 'AIR       ' OR  L_SHIPMODE = 'AIR REG   ' ) "
        "AND ( L_SHIPMODE = 'BBBBBBBBBBBBBBBBBBBBBB' OR  L_SHIPMODE = 'AIR REG   ' ) "
//        "AND L_SHIPINSTRUCT = 'DELIVER IN PERSON        ' "
        "AND L_SHIPINSTRUCT = 'BBBBBBBBBBBBBBBBBBBBBB' "
        ") OR ( "
        "P_BRAND = :brand3 "
//        "AND ( P_CONTAINER = 'LG CASE   ' OR  P_CONTAINER = 'LG BOX    ' OR P_CONTAINER = 'LG PACK   ' OR P_CONTAINER = 'LG PKG    ' ) "
        "AND ( P_CONTAINER = 'BBBBBBBBBBBBBBBBBBBBBB' OR  P_CONTAINER = 'LG BOX    ' OR P_CONTAINER = 'LG PACK   ' OR P_CONTAINER = 'LG PKG    ' ) "
        "AND L_QUANTITY >= :quantity3 AND L_QUANTITY <= :quantity3 + 10 "
        "AND P_SIZE >= 1 AND P_SIZE <= 15 "
//        "AND ( L_SHIPMODE = 'AIR       ' OR  L_SHIPMODE = 'AIR REG   ' ) "
        "AND ( L_SHIPMODE = 'BBBBBBBBBBBBBBBBBBBBBB' OR  L_SHIPMODE = 'AIR REG   ' ) "
//        "AND L_SHIPINSTRUCT = 'DELIVER IN PERSON        ' "
        "AND L_SHIPINSTRUCT = 'BBBBBBBBBBBBBBBBBBBBBB' "
        "))";

    auto ps = api::create_parameter_set();
    set(*ps, "brand1", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "brand2", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "brand3", api::field_type_kind::character, "BBBBBBBBBBBBBBBBBBBBBB"sv);
    set(*ps, "quantity1", api::field_type_kind::int8, 1);
    set(*ps, "quantity2", api::field_type_kind::int8, 1);
    set(*ps, "quantity3", api::field_type_kind::int8, 1);

    std::vector<mock::basic_record> result{};
    execute_query(query, *ps, result);
    ASSERT_EQ(1, result.size());
}

}