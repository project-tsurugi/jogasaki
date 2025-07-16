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
#include "tables.h"

#include <memory>
#include <string_view>

#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/type/with_time_zone.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/common_types.h>
#include <jogasaki/constants.h>
#include <jogasaki/kvs/database.h>

namespace jogasaki::executor {

namespace storage = yugawara::storage;

void add_builtin_tables(storage::configurable_provider& provider) {
    namespace type = ::takatori::type;
    using ::yugawara::variable::nullity;
    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            system_sequences_name,
            {
                { "definition_id", type::int8(), nullity{false} },
                { "sequence_id", type::int8 (), nullity{true} },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
            },
            index_features
        });
    }
}

void add_analytics_benchmark_tables(storage::configurable_provider& provider) {
    namespace type = ::takatori::type;
    using ::yugawara::variable::nullity;
    const static nullity not_null = nullity{false};

    //use 64bit int to avoid implicit type conversion
    //TODO fix when implicit conversion is implemented
    using int_type = type::int8;

    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    {
//        "CREATE TABLE PART ("
//        "P_PARTKEY       BIGINT       NOT NULL, "
//        "P_NAME          VARCHAR(55)  NOT NULL, "
//        "P_MFGR          CHAR(25)     NOT NULL, "
//        "P_BRAND         CHAR(10)     NOT NULL, "
//        "P_TYPE1         VARCHAR(10)  NOT NULL, "
//        "P_TYPE2         VARCHAR(10)  NOT NULL, "
//        "P_TYPE3         VARCHAR(8)  NOT NULL, "
//        "P_SIZE          BIGINT       NOT NULL, "
//        "P_CONTAINER     CHAR(10)     NOT NULL, "
//        "P_RETAILPRICE   BIGINT       NOT NULL, "
//        "P_COMMENT       VARCHAR(23)  NOT NULL, "
//        "PRIMARY KEY(P_PARTKEY))";
        auto t = provider.add_table({
            "PART",
            {
                {"P_PARTKEY", int_type(), not_null},
                {"P_NAME", type::character(type::varying, 55), not_null},
                {"P_MFGR", type::character(25), not_null},
                {"P_BRAND", type::character(10), not_null},
                {"P_TYPE1", type::character(type::varying, 10), not_null},
                {"P_TYPE2", type::character(type::varying, 10), not_null},
                {"P_TYPE3", type::character(type::varying, 8), not_null},
                {"P_SIZE", type::int8(), not_null},
                {"P_CONTAINER", type::character(10), not_null},
                {"P_RETAILPRICE", int_type(), not_null},
                {"P_COMMENT", type::character(type::varying, 23), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
                t->columns()[2],
                t->columns()[3],
                t->columns()[4],
                t->columns()[5],
                t->columns()[6],
                t->columns()[7],
                t->columns()[8],
                t->columns()[9],
                t->columns()[10],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE SUPPLIER ("
//        "S_SUPPKEY       BIGINT       NOT NULL, "
//        "S_NAME          CHAR(25)     NOT NULL, "
//        "S_ADDRESS       VARCHAR(40)  NOT NULL, "
//        "S_NATIONKEY     BIGINT       NOT NULL, "
//        "S_PHONE         CHAR(15)     NOT NULL, "
//        "S_ACCTBAL       BIGINT       NOT NULL, "
//        "S_COMMENT       VARCHAR(101) NOT NULL, "
//        "PRIMARY KEY(S_SUPPKEY))";
        auto t = provider.add_table({
            "SUPPLIER",
            {
                {"S_SUPPKEY", int_type(), not_null},
                {"S_NAME", type::character(25), not_null},
                {"S_ADDRESS", type::character(type::varying, 40), not_null},
                {"S_NATIONKEY", int_type{}, not_null},
                {"S_PHONE", type::character(15), not_null},
                {"S_ACCTBAL", int_type{}, not_null},
                {"S_COMMENT", type::character(type::varying, 101), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
                t->columns()[2],
                t->columns()[3],
                t->columns()[4],
                t->columns()[5],
                t->columns()[6],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE PARTSUPP ("
//        "PS_PARTKEY      BIGINT       NOT NULL, "
//        "PS_SUPPKEY      BIGINT       NOT NULL, "
//        "PS_AVAILQTY     BIGINT       NOT NULL, "
//        "PS_SUPPLYCOST   BIGINT       NOT NULL, "
//        "PS_COMMENT      VARCHAR(199) NOT NULL, "
//        "PRIMARY KEY(PS_PARTKEY, PS_SUPPKEY))";
            auto t = provider.add_table({
            "PARTSUPP",
            {
                {"PS_PARTKEY", int_type(), not_null},
                {"PS_SUPPKEY", int_type(), not_null},
                {"PS_AVAILQTY", int_type(), not_null},
                {"PS_SUPPLYCOST", int_type(), not_null},
                {"PS_COMMENT", type::character(type::varying, 199), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
                t->columns()[1],
            },
            {
                t->columns()[2],
                t->columns()[3],
                t->columns()[4],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE CUSTOMER ("
//        "C_CUSTKEY       BIGINT       NOT NULL, "
//        "C_NAME          VARCHAR(25)  NOT NULL, "
//        "C_ADDRESS       VARCHAR(40)  NOT NULL, "
//        "C_NATIONKEY     BIGINT       NOT NULL, "
//        "C_PHONE         CHAR(15)     NOT NULL, "
//        "C_ACCTBAL       BIGINT       NOT NULL, "
//        "C_MKTSEGMENT    CHAR(10)     NOT NULL, "
//        "C_COMMENT       VARCHAR(117) NOT NULL, "
//        "PRIMARY KEY(C_CUSTKEY))";
            auto t = provider.add_table({
            "CUSTOMER",
            {
                {"C_CUSTKEY", int_type(), not_null},
                {"C_NAME", type::character(type::varying, 25), not_null},
                {"C_ADDRESS", type::character(type::varying, 40), not_null},
                {"C_NATIONKEY", int_type(), not_null},
                {"C_PHONE", type::character(15), not_null},
                {"C_ACCTBAL", int_type(), not_null},
                {"C_MKTSEGMENT", type::character(10), not_null},
                {"C_COMMENT", type::character(type::varying, 117), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
                t->columns()[2],
                t->columns()[3],
                t->columns()[4],
                t->columns()[5],
                t->columns()[6],
                t->columns()[7],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE ORDERS ("
//        "O_ORDERKEY      BIGINT       NOT NULL, "
//        "O_CUSTKEY       BIGINT       NOT NULL, "
//        "O_ORDERSTATUS   CHAR(1)      NOT NULL, "
//        "O_TOTALPRICE    BIGINT       NOT NULL, "
//        "O_ORDERDATE     CHAR(10)     NOT NULL, "
//        "O_ORDERPRIORITY CHAR(15)     NOT NULL, "
//        "O_CLERK         CHAR(15)     NOT NULL, "
//        "O_SHIPPRIORITY  BIGINT       NOT NULL, "
//        "O_COMMENT       VARCHAR(79)  NOT NULL, "
//        "PRIMARY KEY(O_ORDERKEY))";
        auto t = provider.add_table({
            "ORDERS",
            {
                {"O_ORDERKEY", int_type(), not_null},
                {"O_CUSTKEY", int_type(), not_null},
                {"O_ORDERSTATUS", type::character(1), not_null},
                {"O_TOTALPRICE", int_type(), not_null},
                {"O_ORDERDATE", type::character(10), not_null},
                {"O_ORDERPRIORITY", type::character(15), not_null},
                {"O_CLERK", type::character(15), not_null},
                {"O_SHIPPRIORITY", int_type(), not_null},
                {"O_COMMENT", type::character(type::varying, 79), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
                t->columns()[2],
                t->columns()[3],
                t->columns()[4],
                t->columns()[5],
                t->columns()[6],
                t->columns()[7],
                t->columns()[8],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE LINEITEM ("
//        "L_ORDERKEY      BIGINT       NOT NULL, "
//        "L_PARTKEY       BIGINT       NOT NULL, "
//        "L_SUPPKEY       BIGINT       NOT NULL, "
//        "L_LINENUMBER    BIGINT       NOT NULL, "
//        "L_QUANTITY      BIGINT       NOT NULL, "
//        "L_EXTENDEDPRICE BIGINT       NOT NULL, "
//        "L_DISCOUNT      BIGINT       NOT NULL, "
//        "L_TAX           BIGINT       NOT NULL, "
//        "L_RETURNFLAG    CHAR(1)      NOT NULL, "
//        "L_LINESTATUS    CHAR(1)      NOT NULL, "
//        "L_SHIPDATE      CHAR(10)     NOT NULL, "
//        "L_COMMITDATE    CHAR(10)     NOT NULL, "
//        "L_RECEIPTDATE   CHAR(10)     NOT NULL, "
//        "L_SHIPINSTRUCT  CHAR(25)     NOT NULL, "
//        "L_SHIPMODE      CHAR(10)     NOT NULL, "
//        "L_COMMENT       VARCHAR(44)  NOT NULL, "
//        "PRIMARY KEY(L_ORDERKEY,L_LINENUMBER))";
        auto t = provider.add_table({
            "LINEITEM",
            {
                {"L_ORDERKEY", int_type(), not_null},
                {"L_PARTKEY", int_type(), not_null},
                {"L_SUPPKEY", int_type(), not_null},
                {"L_LINENUMBER", int_type(), not_null},
                {"L_QUANTITY", int_type(), not_null},
                {"L_EXTENDEDPRICE", int_type(), not_null},
                {"L_DISCOUNT", int_type(), not_null},
                {"L_TAX", int_type(), not_null},
                {"L_RETURNFLAG", type::character(1), not_null},
                {"L_LINESTATUS", type::character(1), not_null},
                {"L_SHIPDATE", type::character(10), not_null},
                {"L_COMMITDATE", type::character(10), not_null},
                {"L_RECEIPTDATE", type::character(10), not_null},
                {"L_SHIPINSTRUCT", type::character(25), not_null},
                {"L_SHIPMODE", type::character(10), not_null},
                {"L_COMMENT", type::character(type::varying, 44), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
                t->columns()[3],
            },
            {
                t->columns()[1],
                t->columns()[2],
                t->columns()[4],
                t->columns()[5],
                t->columns()[6],
                t->columns()[7],
                t->columns()[8],
                t->columns()[9],
                t->columns()[10],
                t->columns()[11],
                t->columns()[12],
                t->columns()[13],
                t->columns()[14],
                t->columns()[15],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE NATION ("
//        "N_NATIONKEY     BIGINT       NOT NULL, "
//        "N_NAME          CHAR(25)     NOT NULL, "
//        "N_REGIONKEY     BIGINT       NOT NULL, "
//        "N_COMMENT       VARCHAR(152) NOT NULL, "
//        "PRIMARY KEY(N_NATIONKEY))";
            auto t = provider.add_table({
            "NATION",
            {
                {"N_NATIONKEY", int_type(), not_null},
                {"N_NAME", type::character(25), not_null},
                {"N_REGIONKEY", int_type(), not_null},
                {"N_COMMENT", type::character(type::varying, 152), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
                t->columns()[2],
                t->columns()[3],
            },
            index_features
        });
    }{
//        "CREATE TABLE REGION ("
//        "R_REGIONKEY     BIGINT       NOT NULL, "
//        "R_NAME          CHAR(25)     NOT NULL, "
//        "R_COMMENT       VARCHAR(152) NOT NULL, "
//        "PRIMARY KEY(R_REGIONKEY))";
        auto t = provider.add_table({
            "REGION",
            {
                {"R_REGIONKEY", int_type(), not_null},
                {"R_NAME", type::character(25), not_null},
                {"R_COMMENT", type::character(type::varying, 152), not_null},
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
                t->columns()[2],
            },
            index_features
        });
    }
}

void register_kvs_storage(kvs::database& db, yugawara::storage::configurable_provider& provider) {
    provider.each_index([&db](std::string_view id, std::shared_ptr<yugawara::storage::index const> const& ){
        auto stg = db.get_or_create_storage(id);
    });
}

}
