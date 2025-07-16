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
#include "tables.h"

#include <memory>
#include <string_view>

#include <takatori/type/blob.h>
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

namespace jogasaki::utils {

namespace storage = yugawara::storage;

// built-in sequences definition ids
constexpr static sequence_definition_id h_id_sequence = 0;

void add_benchmark_tables(storage::configurable_provider& provider) {
    namespace type = ::takatori::type;
    using ::yugawara::variable::nullity;
    const static nullity not_null = nullity{false};
    const static nullity nullable = nullity{true};

    //use 64bit int to avoid implicit type conversion
    //TODO fix when implicit conversion is implemented
    using int_type = type::int8;

    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    yugawara::storage::index_feature_set secondary_index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
    };
    {
//        "CREATE TABLE WAREHOUSE ("
//        "w_id INT NOT NULL, "
//        "w_name VARCHAR(10) NOT NULL, "
//        "w_street_1 VARCHAR(20) NOT NULL, "
//        "w_street_2 VARCHAR(20) NOT NULL, "
//        "w_city VARCHAR(20) NOT NULL, "
//        "w_state CHAR(2) NOT NULL, "
//        "w_zip CHAR(9) NOT NULL, "
//        "w_tax DOUBLE NOT NULL, "
//        "w_ytd DOUBLE NOT NULL, "
//        "PRIMARY KEY(w_id))",
        auto t = provider.add_table({
            "WAREHOUSE",
            {
                { "w_id", int_type(), not_null },
                { "w_name", type::character(type::varying, 10), not_null },
                { "w_street_1", type::character(type::varying, 20), not_null },
                { "w_street_2", type::character(type::varying, 20), not_null },
                { "w_city", type::character(type::varying, 20), not_null },
                { "w_state", type::character(2), not_null },
                { "w_zip", type::character(9), not_null },
                { "w_tax", type::float8(), not_null },
                { "w_ytd", type::float8(), not_null },
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
//        "CREATE TABLE DISTRICT ("
//        "d_id INT NOT NULL, "
//        "d_w_id INT NOT NULL, "
//        "d_name VARCHAR(10) NOT NULL, "
//        "d_street_1 VARCHAR(20) NOT NULL, "
//        "d_street_2 VARCHAR(20) NOT NULL, "
//        "d_city VARCHAR(20) NOT NULL, "
//        "d_state CHAR(2) NOT NULL, "
//        "d_zip  CHAR(9) NOT NULL, "
//        "d_tax DOUBLE NOT NULL, "
//        "d_ytd DOUBLE NOT NULL, "
//        "d_next_o_id INT NOT NULL, "
//        "PRIMARY KEY(d_w_id, d_id))",
        auto t = provider.add_table({
            "DISTRICT",
            {
                { "d_id", int_type(), not_null },
                { "d_w_id", int_type(), not_null },
                { "d_name", type::character(type::varying, 10), not_null },
                { "d_street_1", type::character(type::varying, 20), not_null },
                { "d_street_2", type::character(type::varying, 20), not_null },
                { "d_city", type::character(type::varying, 20), not_null },
                { "d_state", type::character(2), not_null },
                { "d_zip", type::character(9), not_null },
                { "d_tax", type::float8(), not_null },
                { "d_ytd", type::float8(), not_null },
                { "d_next_o_id", int_type(), not_null },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[1],
                t->columns()[0],
            },
            {
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
//        "CREATE TABLE CUSTOMER ("
//        "c_id INT NOT NULL, "
//        "c_d_id INT NOT NULL, "
//        "c_w_id INT NOT NULL, "
//        "c_first VARCHAR(16) NOT NULL, "
//        "c_middle CHAR(2) NOT NULL, "
//        "c_last VARCHAR(16) NOT NULL, "
//        "c_street_1 VARCHAR(20) NOT NULL, "
//        "c_street_2 VARCHAR(20) NOT NULL, "
//        "c_city VARCHAR(20) NOT NULL, "
//        "c_state CHAR(2) NOT NULL, "
//        "c_zip  CHAR(9) NOT NULL, "
//        "c_phone CHAR(16) NOT NULL, "
//        "c_since CHAR(25) NOT NULL, " // date
//        "c_credit CHAR(2) NOT NULL, "
//        "c_credit_lim DOUBLE NOT NULL, "
//        "c_discount DOUBLE NOT NULL, "
//        "c_balance DOUBLE NOT NULL, "
//        "c_ytd_payment DOUBLE NOT NULL, "
//        "c_payment_cnt INT NOT NULL, "
//        "c_delivery_cnt INT NOT NULL, "
//        "c_data VARCHAR(500) NOT NULL, "
//        "PRIMARY KEY(c_w_id, c_d_id, c_id))",
        auto t = provider.add_table({
            "CUSTOMER",
            {
                { "c_id", int_type(), not_null },
                { "c_d_id", int_type(), not_null },
                { "c_w_id", int_type(), not_null },
                { "c_first", type::character(type::varying, 16), not_null },
                { "c_middle", type::character(2), not_null },
                { "c_last", type::character(type::varying, 16), not_null },
                { "c_street_1", type::character(type::varying, 20), not_null },
                { "c_street_2", type::character(type::varying, 20), not_null },
                { "c_city", type::character(type::varying, 20), not_null },
                { "c_state", type::character(2), not_null },
                { "c_zip", type::character(9), not_null },
                { "c_phone", type::character(16), not_null },
                { "c_since", type::character(25), not_null },
                { "c_credit", type::character(2), not_null },
                { "c_credit_lim", type::float8(), not_null },
                { "c_discount", type::float8(), not_null },
                { "c_balance", type::float8(), not_null },
                { "c_ytd_payment", type::float8(), not_null },
                { "c_payment_cnt", int_type(), not_null },
                { "c_delivery_cnt", int_type(), not_null },
                { "c_data", type::character(type::varying, 500), not_null },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[2],
                t->columns()[1],
                t->columns()[0],
            },
            {
                t->columns()[3],
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
                t->columns()[16],
                t->columns()[17],
                t->columns()[18],
                t->columns()[19],
                t->columns()[20],
            },
            index_features
        });
//        "PRIMARY KEY(c_w_id, c_d_id, c_last, c_first))",
        provider.add_index({
            t,
            "CUSTOMER_SECONDARY",
            {
                t->columns()[2],
                t->columns()[1],
                t->columns()[5],
                t->columns()[3],
            },
            {},
            secondary_index_features
        });
    }
    {
//        "CREATE TABLE NEW_ORDER ("
//        "no_o_id INT NOT NULL, "
//        "no_d_id INT NOT NULL, "
//        "no_w_id INT NOT NULL, "
//        "PRIMARY KEY(no_w_id, no_d_id, no_o_id))",
        auto t = provider.add_table({
            "NEW_ORDER",
            {
                { "no_o_id", int_type(), not_null },
                { "no_d_id", int_type(), not_null },
                { "no_w_id", int_type(), not_null },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[2],
                t->columns()[1],
                t->columns()[0],
            },
            {
            },
            index_features
        });
    }
    {
//        "CREATE TABLE ORDERS (" // ORDER is a reserved word of SQL
//        "o_id INT NOT NULL, "
//        "o_d_id INT NOT NULL, "
//        "o_w_id INT NOT NULL, "
//        "o_c_id INT NOT NULL, "
//        "o_entry_d CHAR(25) NOT NULL, " // date
//        "o_carrier_id INT, " // nullable
//        "o_ol_cnt INT NOT NULL, "
//        "o_all_local INT NOT NULL, "
//        "PRIMARY KEY(o_w_id, o_d_id, o_id))",
        auto t = provider.add_table({
            "ORDERS",
            {
                { "o_id", int_type(), not_null },
                { "o_d_id", int_type(), not_null },
                { "o_w_id", int_type(), not_null },
                { "o_c_id", int_type(), not_null },
                { "o_entry_d", type::character(25), not_null },
                { "o_carrier_id", int_type(), nullable },
                { "o_ol_cnt", int_type(), not_null },
                { "o_all_local", int_type(), not_null },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[2],
                t->columns()[1],
                t->columns()[0],
            },
            {
                t->columns()[3],
                t->columns()[4],
                t->columns()[5],
                t->columns()[6],
                t->columns()[7],
            },
            index_features
        });
//        "PRIMARY KEY(o_w_id, o_d_id, o_c_id, o_id))",
        provider.add_index({
            t,
            "ORDERS_SECONDARY",
            {
                t->columns()[2],
                t->columns()[1],
                t->columns()[3],
                t->columns()[0],
            },
            {},
            secondary_index_features
        });
    }
    {
//        "CREATE TABLE ORDER_LINE ("
//        "ol_o_id INT NOT NULL, "
//        "ol_d_id INT NOT NULL, "
//        "ol_w_id INT NOT NULL, "
//        "ol_number INT NOT NULL, "
//        "ol_i_id INT NOT NULL, "
//        "ol_supply_w_id INT NOT NULL, "
//        "ol_delivery_d CHAR(25), " // date, nullable
//        "ol_quantity INT NOT NULL, "
//        "ol_amount DOUBLE NOT NULL, "
//        "ol_dist_info CHAR(24) NOT NULL, "
//        "PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number))",
        auto t = provider.add_table({
            "ORDER_LINE",
            {
                { "ol_o_id", int_type(), not_null },
                { "ol_d_id", int_type(), not_null },
                { "ol_w_id", int_type(), not_null },
                { "ol_number", int_type(), not_null },
                { "ol_i_id", int_type(), not_null },
                { "ol_supply_w_id", int_type(), not_null },
                { "ol_delivery_d", type::character(25), nullable },
                { "ol_quantity", int_type(), not_null },
                { "ol_amount", type::float8(), not_null },
                { "ol_dist_info", type::character(24), not_null },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[2],
                t->columns()[1],
                t->columns()[0],
                t->columns()[3],
            },
            {
                t->columns()[4],
                t->columns()[5],
                t->columns()[6],
                t->columns()[7],
                t->columns()[8],
                t->columns()[9],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE ITEM ("
//        "i_id INT NOT NULL, "
//        "i_im_id INT, " // not used
//        "i_name VARCHAR(24) NOT NULL, "
//        "i_price DOUBLE NOT NULL, "
//        "i_data VARCHAR(50) NOT NULL, "
//        "PRIMARY KEY(i_id))",
        auto t = provider.add_table({
            "ITEM",
            {
                { "i_id", int_type(), not_null },
                { "i_im_id", int_type(), not_null },
                { "i_name", type::character(type::varying, 24), not_null },
                { "i_price", type::float8(), not_null },
                { "i_data", type::character(type::varying, 50), not_null },
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
            },
            index_features
        });
    }
    {
//        "CREATE TABLE STOCK ("
//        "s_i_id INT NOT NULL, "
//        "s_w_id INT NOT NULL, "
//        "s_quantity INT NOT NULL, "
//        "s_dist_01 CHAR(24) NOT NULL, "
//        "s_dist_02 CHAR(24) NOT NULL, "
//        "s_dist_03 CHAR(24) NOT NULL, "
//        "s_dist_04 CHAR(24) NOT NULL, "
//        "s_dist_05 CHAR(24) NOT NULL, "
//        "s_dist_06 CHAR(24) NOT NULL, "
//        "s_dist_07 CHAR(24) NOT NULL, "
//        "s_dist_08 CHAR(24) NOT NULL, "
//        "s_dist_09 CHAR(24) NOT NULL, "
//        "s_dist_10 CHAR(24) NOT NULL, "
//        "s_ytd INT NOT NULL, "
//        "s_order_cnt INT NOT NULL, "
//        "s_remote_cnt INT NOT NULL, "
//        "s_data VARCHAR(50) NOT NULL, "
//        "PRIMARY KEY(s_w_id, s_i_id))",
        auto t = provider.add_table({
            "STOCK",
            {
                { "s_i_id", int_type(), not_null },
                { "s_w_id", int_type(), not_null },
                { "s_quantity", int_type(), not_null },
                { "s_dist_01", type::character(24), not_null },
                { "s_dist_02", type::character(24), not_null },
                { "s_dist_03", type::character(24), not_null },
                { "s_dist_04", type::character(24), not_null },
                { "s_dist_05", type::character(24), not_null },
                { "s_dist_06", type::character(24), not_null },
                { "s_dist_07", type::character(24), not_null },
                { "s_dist_08", type::character(24), not_null },
                { "s_dist_09", type::character(24), not_null },
                { "s_dist_10", type::character(24), not_null },
                { "s_ytd", int_type(), not_null },
                { "s_order_cnt", int_type(), not_null },
                { "s_remote_cnt", int_type(), not_null },
                { "s_data", type::character(type::varying, 50), not_null },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[1],
                t->columns()[0],
            },
            {
                t->columns()[2],
                t->columns()[3],
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
                t->columns()[16],
            },
            index_features
        });
    }
    {
//        "CREATE TABLE HISTORY ("
//        "h_c_id INT NOT NULL,"
//        "h_c_d_id INT NOT NULL,"
//        "h_c_w_id INT NOT NULL,"
//        "h_d_id INT NOT NULL,"
//        "h_w_id INT NOT NULL,"
//        "h_date CHAR(25) NOT NULL, " // date
//        "h_amount DOUBLE NOT NULL, "
//        "h_data CHAR(24) NOT NULL, "
//        ")",
        auto s1 = std::make_shared<storage::sequence>(
            h_id_sequence,
            "h_id_sequence"
        );
        provider.add_sequence(s1);
        auto t = provider.add_table({
            "HISTORY",
            {
                { "h_id", int_type(), not_null, {s1} },  // generated by sequence
                { "h_c_id", int_type(), not_null },
                { "h_c_d_id", int_type(), not_null },
                { "h_c_w_id", int_type(), not_null },
                { "h_d_id", int_type(), not_null },
                { "h_w_id", int_type(), not_null },
                { "h_date", type::character(25), not_null },
                { "h_amount", type::float8(), not_null },
                { "h_data", type::character(type::varying, 24), not_null },
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
}

void add_test_tables(storage::configurable_provider& provider) {
    namespace type = ::takatori::type;
    using ::yugawara::variable::nullity;
    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    yugawara::storage::index_feature_set secondary_index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
    };
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "T0",
            {
                { "C0", type::int8(), nullity{false} },
                { "C1", type::float8 (), nullity{true} },
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
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "T1",
            {
                { "C0", type::int4(), nullity{false} },
                { "C1", type::int8(), nullity{true}  },
                { "C2", type::float8() , nullity{true} },
                { "C3", type::float4() , nullity{true} },
                { "C4", type::character(type::varying, 100) , nullity{true} },
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
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "NON_NULLABLES",
            {
                { "K0", type::int8(), nullity{false}  },
                { "C0", type::int4(), nullity{false} },
                { "C1", type::int8(), nullity{false}  },
                { "C2", type::float8(), nullity{false} },
                { "C3", type::float4(), nullity{false} },
                { "C4", type::character(type::varying, 100), nullity{false} },
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
            },
            index_features
        });
    }
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "T2",
            {
                { "C0", type::int4(), nullity{false} },
                { "C1", type::int8(), nullity{true}  },
                { "C2", type::float8() , nullity{true} },
                { "C3", type::float4() , nullity{true} },
                { "C4", type::character(type::varying, 100) , nullity{true} },
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
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "T10",
            {
                { "C0", type::int8(), nullity{false} },
                { "C1", type::float8 (), nullity{true} },
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
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "T20",
            {
                { "C0", type::int8(), nullity{false} },
                { "C1", type::int4(), nullity{true}  },
                { "C2", type::float8() , nullity{true} },
                { "C3", type::float4() , nullity{true} },
                { "C4", type::character(type::varying, 100) , nullity{true} },
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
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "INT4_TAB",
            {
                { "C0", type::int4(), nullity{false} },
                { "C1", type::int4(), nullity{true} },
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
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "CHAR_TAB",
            {
                { "C0", type::int8(), nullity{false} },
                { "VC", type::character(type::varying, 5) , nullity{true} },
                { "CH", type::character(~type::varying, 5) , nullity{true} },
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
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "TTEMPORALS",
            {
                { "K0", type::date(), nullity{false} },
                { "K1", type::time_of_day(), nullity{false} },
                { "K2", type::time_of_day(type::with_time_zone_t{true}), nullity{false} },
                { "K3", type::time_point(), nullity{false} },
                { "K4", type::time_point(type::with_time_zone_t{true}), nullity{false} },
                { "C0", type::date(), nullity{true} },
                { "C1", type::time_of_day(), nullity{true} },
                { "C2", type::time_of_day(type::with_time_zone_t{true}), nullity{true} },
                { "C3", type::time_point(), nullity{true} },
                { "C4", type::time_point(type::with_time_zone_t{true}), nullity{true} },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
                t->columns()[1],
                t->columns()[2],
                t->columns()[3],
                t->columns()[4],
            },
            {
                t->columns()[5],
                t->columns()[6],
                t->columns()[7],
                t->columns()[8],
                t->columns()[9],
            },
            index_features
        });
    }
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "TDECIMALS",
            {
                { "K0", type::decimal(3, 0), nullity{false} },
                { "K1", type::decimal(5, 3), nullity{false} },
                { "K2", type::decimal(10,1), nullity{false} },
                { "C0", type::decimal(3, 0), nullity{true} },
                { "C1", type::decimal(5, 3), nullity{true} },
                { "C2", type::decimal(10,1), nullity{true} },
            },
        });
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
                t->columns()[1],
                t->columns()[2],
            },
            {
                t->columns()[3],
                t->columns()[4],
                t->columns()[5],
            },
            index_features
        });
    }
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "TSECONDARY",
            {
                { "C0", type::int8(), nullity{false} },
                { "C1", type::int8(), nullity{true} },
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
        provider.add_index({
            t,
            "TSECONDARY_I1",
            {
                t->columns()[1],
            },
            {},
            secondary_index_features
        });
    }
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "TLOB",
            {
                { "C0", type::int4(), nullity{false} },
                { "C1", type::blob(), nullity{true} },
                { "C2", type::clob(), nullity{true} },
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

}  // namespace jogasaki::utils
