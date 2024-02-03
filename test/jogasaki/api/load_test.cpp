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

#include <future>
#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/data/any.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/storage_data.h>

#include "../test_utils/temporary_folder.h"
#include "api_test_base.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using impl::get_impl;

inline std::shared_ptr<jogasaki::meta::external_record_meta> create_file_meta() {
    return std::make_shared<meta::external_record_meta>(
        std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                meta::field_type(std::make_shared<meta::character_field_option>()),
            },
            boost::dynamic_bitset<std::uint64_t>{1}.flip()
        ),
        std::vector<std::optional<std::string>>{"file_name"}
    );
}

class load_test :
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
        temporary_.prepare();
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }

    void test_dump(
        std::string_view sql,
        std::vector<std::string>& files,
        std::size_t max_records_per_file = 0
    ) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(sql, stmt));
        explain(*stmt);
        auto transaction = utils::create_transaction(*db_);
        auto tx = get_impl(*db_).find_transaction(*transaction);
        status s{};
        std::string message{"message"};
        std::atomic_bool run{false};
        test_channel ch{};
        io::dump_config opts{};
        opts.max_records_per_file_ = max_records_per_file;
        ASSERT_TRUE(executor::execute_dump(
            get_impl(*db_),
            tx,
            maybe_shared_ptr{stmt.get()},
            maybe_shared_ptr{&ch},
            path(),
            [&](status st, std::shared_ptr<error::error_info> info){
                s = st;
                message = (info ? info->message() : "");
                run.store(true);
            },
            opts
        ));
        while(! run.load()) {}
        ASSERT_EQ(status::ok, s);
        ASSERT_TRUE(message.empty());
        auto& wrt = ch.writers_[0];
        ASSERT_TRUE(stmt->meta());
        auto m = create_file_meta();
        auto recs = deserialize_msg({wrt->data_.data(), wrt->size_}, *m->origin());
        ASSERT_LT(0, recs.size());
        for(auto&& x : recs) {
            files.emplace_back(static_cast<std::string>(x.get_value<accessor::text>(0)));
            LOG(INFO) << x;
        }
        EXPECT_TRUE(ch.all_writers_released());
        ASSERT_EQ(status::ok, executor::commit(get_impl(*db_), tx));
    }

    void test_load(
        std::vector<std::string> const& files,
        std::string_view statement,
        std::unordered_map<std::string, api::field_type_kind>& variables,
        std::unique_ptr<api::parameter_set> ps,
        status expected = status::ok
    ) {
        auto transaction = utils::create_transaction(*db_);
        auto tx = get_impl(*db_).find_transaction(*transaction);
        status s{};
        std::string message{"message"};
        std::atomic_bool run{false};

        api::statement_handle prepared{};
        ASSERT_EQ(status::ok, db_->prepare(statement, variables, prepared));

        ASSERT_TRUE(executor::execute_load(
            get_impl(*db_),
            tx,
            prepared,
            std::shared_ptr{std::move(ps)},
            files,
            [&](status st, std::shared_ptr<error::error_info> info){
                s = st;
                message = (info ? info->message() : "");
                run.store(true);
            }
        ));

        while(! run.load()) {}
        ASSERT_EQ(expected, s);
        if (expected == status::ok) {
            ASSERT_EQ(status::ok, executor::commit(get_impl(*db_), tx));
        } else {
            ASSERT_EQ(status::ok, executor::abort_transaction(tx));
        }
    }

    void test_load(
        std::vector<std::string> const& files, status expected = status::ok
    ) {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int8},
            {"p1", api::field_type_kind::float8},
        };
        auto ps = api::create_parameter_set();
        ps->set_float8("p1", 1.0);
        ps->set_reference_column("p0", "C0");
        test_load(files, "INSERT INTO T0(C0, C1) VALUES (:p0, :p1)", variables, std::move(ps), expected);
    }
};

using namespace std::string_view_literals;

TEST_F(load_test, basic) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::vector<std::string> files{};
    test_dump("select * from T0", files);
    execute_statement( "DELETE FROM T0");
    test_load(files);
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(2,1.0)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(3,1.0)), result[2]);
    }
}

TEST_F(load_test, wrong_file) {
    std::vector<std::string> files{"dummy.parquet"};
    test_load(files, status::err_aborted);
}

TEST_F(load_test, existing_file_and_missing_file) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on abort";
    }
    // verify load failed with missing file and transaction abort, no records are loaded eventually
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::vector<std::string> files{};
    test_dump("select * from T0", files);
    execute_statement( "DELETE FROM T0");
    files.emplace_back("dummy.parquet");
    test_load(files, status::err_aborted);
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 ORDER BY C0", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(load_test, decimals) {
    auto v111 = decimal_v{1, 0, 111, 0}; // 111
    auto v11_111 = decimal_v{1, 0, 11111, -3}; // 11.111
    auto v11111_1 = decimal_v{1, 0, 111111, -1}; // 11111.1
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::decimal},
            {"p1", api::field_type_kind::decimal},
            {"p2", api::field_type_kind::decimal},
        };
        auto ps = api::create_parameter_set();

        ps->set_decimal("p0", v111);
        ps->set_decimal("p1", v11_111);
        ps->set_decimal("p2", v11111_1);
        execute_statement("INSERT INTO TDECIMALS (K0, K1, K2, C0, C1, C2) VALUES (:p0, :p1, :p2, :p0, :p1, :p2)", variables, *ps);
    }

    std::vector<std::string> files{};
    test_dump("select * from TDECIMALS", files);
    execute_statement("DELETE FROM TDECIMALS");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal},
        {"p2", api::field_type_kind::decimal},
        {"p3", api::field_type_kind::decimal},
        {"p4", api::field_type_kind::decimal},
        {"p5", api::field_type_kind::decimal},
    };
    auto ps = api::create_parameter_set();
    ps->set_reference_column("p0", "K0");
    ps->set_reference_column("p1", "K1");
    ps->set_reference_column("p2", "K2");
    ps->set_reference_column("p3", "C0");
    ps->set_reference_column("p4", "C1");
    ps->set_reference_column("p5", "C2");

    test_load(files, "INSERT INTO TDECIMALS (K0, K1, K2, C0, C1, C2) VALUES (:p0, :p1, :p2, :p3, :p4, :p5)", variables, std::move(ps));
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TDECIMALS ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        auto dec_3_0 = meta::field_type{std::make_shared<meta::decimal_field_option>(3, 0)};
        auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
        auto dec_10_1 = meta::field_type{std::make_shared<meta::decimal_field_option>(10, 1)};
        EXPECT_EQ((mock::typed_nullable_record<
            kind::decimal, kind::decimal, kind::decimal,
            kind::decimal, kind::decimal, kind::decimal
        >(
            std::tuple{
                dec_3_0, dec_5_3, dec_10_1,
                dec_3_0, dec_5_3, dec_10_1,
            },
            {
                v111, v11_111, v11111_1,
                v111, v11_111, v11111_1,
            }
        )), result[0]);
    }
}

TEST_F(load_test, decimals_with_indefinite_precscale) {
    auto v1 = decimal_v{1, 0, 1, 0}; // 1
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::decimal},
            {"p1", api::field_type_kind::decimal},
            {"p2", api::field_type_kind::decimal},
        };
        auto ps = api::create_parameter_set();

        ps->set_decimal("p0", v1);
        ps->set_decimal("p1", v1);
        ps->set_decimal("p2", v1);
        execute_statement("INSERT INTO TDECIMALS (K0, K1, K2, C0, C1, C2) VALUES (:p0, :p1, :p2, :p0, :p1, :p2)", variables, *ps);
    }

    std::vector<std::string> files{};
    test_dump("select K0*K0 as K0, K1*K1 as K1, K2*K2 as K2, C0*C0 as C0, C1*C1 as C1, C2*C2 as C2 from TDECIMALS", files);
    execute_statement("DELETE FROM TDECIMALS");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal},
        {"p2", api::field_type_kind::decimal},
        {"p3", api::field_type_kind::decimal},
        {"p4", api::field_type_kind::decimal},
        {"p5", api::field_type_kind::decimal},
    };
    auto ps = api::create_parameter_set();
    ps->set_reference_column("p0", "K0");
    ps->set_reference_column("p1", "K1");
    ps->set_reference_column("p2", "K2");
    ps->set_reference_column("p3", "C0");
    ps->set_reference_column("p4", "C1");
    ps->set_reference_column("p5", "C2");

    test_load(files, "INSERT INTO TDECIMALS (K0, K1, K2, C0, C1, C2) VALUES (:p0, :p1, :p2, :p3, :p4, :p5)", variables, std::move(ps));
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TDECIMALS ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        auto dec_3_0 = meta::field_type{std::make_shared<meta::decimal_field_option>(3, 0)};
        auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
        auto dec_10_1 = meta::field_type{std::make_shared<meta::decimal_field_option>(10, 1)};
        EXPECT_EQ((mock::typed_nullable_record<
            kind::decimal, kind::decimal, kind::decimal,
            kind::decimal, kind::decimal, kind::decimal
        >(
            std::tuple{
                dec_3_0, dec_5_3, dec_10_1,
                dec_3_0, dec_5_3, dec_10_1,
            },
            {
                v1, v1, v1,
                v1, v1, v1,
            }
        )), result[0]);
    }
}

TEST_F(load_test, cast_from_string) {
    execute_statement("create table TT (C0 int primary key, C1 bigint, C2 float, C3 double, C4 decimal(5,3))");
    execute_statement("create table SRC (C0 VARCHAR(10), C1 VARCHAR(10), C2 VARCHAR(10), C3 VARCHAR(10), C4 VARCHAR(10))");
    execute_statement("INSERT INTO SRC (C0, C1, C2, C3, C4) VALUES ('1', '10', '100.0', '1000.0', '11.111')");
    execute_statement("INSERT INTO SRC (C0, C1, C2, C3, C4) VALUES ('2', '20', '200.0', '2000.0', '22.222')");

    std::vector<std::string> files{};
    test_dump("select C0, C1, C2, C3, C4 from SRC", files);

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character},
        {"p1", api::field_type_kind::character},
        {"p2", api::field_type_kind::character},
        {"p3", api::field_type_kind::character},
        {"p4", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    ps->set_reference_column("p0", "C0");
    ps->set_reference_column("p1", "C1");
    ps->set_reference_column("p2", "C2");
    ps->set_reference_column("p3", "C3");
    ps->set_reference_column("p4", "C4");

    test_load(files, "INSERT INTO TT (C0, C1, C2, C3, C4) VALUES (CAST(:p0 AS INT), CAST(:p1 AS BIGINT), CAST(:p2 AS FLOAT), CAST(:p3 AS DOUBLE), CAST(:p4 AS DECIMAL(5,3)))", variables, std::move(ps));
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8, kind::decimal>(
            std::tuple{
                meta::field_type{meta::field_enum_tag<kind::int4>},
                meta::field_type{meta::field_enum_tag<kind::int8>},
                meta::field_type{meta::field_enum_tag<kind::float4>},
                meta::field_type{meta::field_enum_tag<kind::float8>},
                meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)},
            },
            { 1, 10, 100.0, 1000.0, decimal_v{1, 0, 11111, -3} /* 11.111 */ }
        )), result[0]);
        EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8, kind::decimal>(
            std::tuple{
                meta::field_type{meta::field_enum_tag<kind::int4>},
                meta::field_type{meta::field_enum_tag<kind::int8>},
                meta::field_type{meta::field_enum_tag<kind::float4>},
                meta::field_type{meta::field_enum_tag<kind::float8>},
                meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)},
            },
            { 2, 20, 200.0, 2000.0, decimal_v{1, 0, 22222, -3} /* 22.222 */ }
        )), result[1]);
    }
}

// create better way to place custom parquet files
TEST_F(load_test, DISABLED_third_party_file) {
    std::vector<std::string> files{
        "INT_DOUBLE.parquet"
    };

    execute_statement("create table TT (C0 double, C1 int)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::float8},
        {"p1", api::field_type_kind::int4},
    };
    auto ps = api::create_parameter_set();
    ps->set_reference_column("p0", "Z");
    ps->set_reference_column("p1", "Inten_sity");

    test_load(files, "INSERT INTO TT (C0, C1) VALUES (:p0, :p1)", variables, std::move(ps));
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT ORDER BY C0", result);
        ASSERT_EQ(5, result.size());
    }
}

}  // namespace jogasaki::api
