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

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/datastore/blob_pool_mock.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/create_file.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

#include <jogasaki/api/transaction_handle_internal.h>

namespace jogasaki::testing {
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;

using takatori::util::unsafe_downcast;

class blob_type_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        datastore::get_datastore(true);  // reset cache for datastore object as db setup recreates it
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;
using kind = meta::field_type_kind;

TEST_F(blob_type_test, insert_provided) {
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "\x00\x01\x02"sv);
    create_file(path2, "ABC");

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    execute_statement("INSERT INTO t VALUES (:p0, :p1, :p2)", variables, *ps);
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore();
    auto ret1 = ds->get_blob_file(ref1.object_id());
    ASSERT_TRUE(ret1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1.path().string())) << ret1.path().string();
    auto ret2 = ds->get_blob_file(ref2.object_id());
    ASSERT_TRUE(ret2);
    EXPECT_EQ("ABC", read_file(ret2.path().string())) << ret2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);
    EXPECT_EQ(status::ok, tx->commit());
}

TEST_F(blob_type_test, blob_pool_release) {
    // verify blob pool is correctly released when transaction completes
    global::config_pool()->mock_datastore(true); // this test requires mock datastore
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
        };

    auto path1 = path()+"/blob_types1.dat";
    create_file(path1, "\x00\x01\x02"sv);

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    std::shared_ptr<limestone::api::blob_pool> pool{};
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("INSERT INTO t VALUES (:p0, :p1)", variables, *ps, *tx);
        auto tctx = api::get_transaction_context(*tx);
        pool = tctx->blob_pool();
        EXPECT_TRUE(pool);
        EXPECT_TRUE(! static_cast<datastore::blob_pool_mock&>(*pool).released());
        EXPECT_EQ(status::ok, tx->commit());
    }
    wait_epochs(); // tx context might not be destroyed very soon
    EXPECT_TRUE(static_cast<datastore::blob_pool_mock&>(*pool).released());
}

TEST_F(blob_type_test, update) {
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "ABC");
    create_file(path2, "DEF");

    auto path3 = path()+"/file3.dat";
    auto path4 = path()+"/file4.dat";
    create_file(path3, "abc");
    create_file(path4, "def");

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_blob("p1", lob::blob_locator{path1});
        ps->set_clob("p2", lob::clob_locator{path2});
        execute_statement("INSERT INTO t VALUES (:p0, :p1, :p2)", variables, *ps);
    }
    lob::lob_id_type id1;
    lob::lob_id_type id2;
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1_src = result[0].get_value<lob::blob_reference>(1);
        id1 = ref1_src.object_id();
        auto ref2_src = result[0].get_value<lob::clob_reference>(2);
        id2 = ref2_src.object_id();
        EXPECT_EQ(status::ok, tx->commit());
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_blob("p1", lob::blob_locator{path3});
        ps->set_clob("p2", lob::clob_locator{path4});
        execute_statement("UPDATE t SET c1 = :p1, c2 = :p2 WHERE c0 = :p0", variables, *ps);
    }

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore(false);
    auto ret1 = ds->get_blob_file(ref1.object_id());
    ASSERT_TRUE(ret1);
    EXPECT_EQ("abc", read_file(ret1.path().string())) << ret1.path().string();
    auto ret2 = ds->get_blob_file(ref2.object_id());
    ASSERT_TRUE(ret2);
    EXPECT_EQ("def", read_file(ret2.path().string())) << ret2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);
    EXPECT_EQ(status::ok, tx->commit());
    EXPECT_NE(id1, ref1.object_id());
    EXPECT_NE(id2, ref2.object_id());
}

TEST_F(blob_type_test, update_partially) {
    // update some blob column while keeping the other unchanged
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "ABC");
    create_file(path2, "DEF");

    auto path3 = path()+"/file3.dat";
    create_file(path3, "abc");

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_blob("p1", lob::blob_locator{path1});
        ps->set_clob("p2", lob::clob_locator{path2});
        execute_statement("INSERT INTO t VALUES (:p0, :p1, :p2)", variables, *ps);
    }
    lob::lob_id_type id1;
    lob::lob_id_type id2;
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1_src = result[0].get_value<lob::blob_reference>(1);
        id1 = ref1_src.object_id();
        auto ref2_src = result[0].get_value<lob::clob_reference>(2);
        id2 = ref2_src.object_id();
        EXPECT_EQ(status::ok, tx->commit());
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_blob("p1", lob::blob_locator{path3});
        execute_statement("UPDATE t SET c1 = :p1 WHERE c0 = :p0", variables, *ps);
    }

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore(false);
    auto ret1 = ds->get_blob_file(ref1.object_id());
    ASSERT_TRUE(ret1);
    EXPECT_EQ("abc", read_file(ret1.path().string())) << ret1.path().string();
    auto ret2 = ds->get_blob_file(ref2.object_id());
    ASSERT_TRUE(ret2);
    EXPECT_EQ("DEF", read_file(ret2.path().string())) << ret2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);
    EXPECT_EQ(status::ok, tx->commit());

    EXPECT_NE(id1, ref1.object_id());
    EXPECT_NE(id2, ref2.object_id());
}

TEST_F(blob_type_test, insert_from_select) {
    // insert blob column from select
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table src (c0 int primary key, c1 blob, c2 clob)");
    execute_statement("create table dest (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "ABC");
    create_file(path2, "DEF");

    auto path3 = path()+"/file3.dat";
    create_file(path3, "abc");

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_blob("p1", lob::blob_locator{path1});
        ps->set_clob("p2", lob::clob_locator{path2});
        execute_statement("INSERT INTO src VALUES (:p0, :p1, :p2)", variables, *ps);
    }
    execute_statement("INSERT INTO dest SELECT c0, c1, c2 from src");

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM dest", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore(false);
    auto ret1 = ds->get_blob_file(ref1.object_id());
    ASSERT_TRUE(ret1);
    EXPECT_EQ("ABC", read_file(ret1.path().string())) << ret1.path().string();
    auto ret2 = ds->get_blob_file(ref2.object_id());
    ASSERT_TRUE(ret2);
    EXPECT_EQ("DEF", read_file(ret2.path().string())) << ret2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);
    EXPECT_EQ(status::ok, tx->commit());

    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM src", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1_src = result[0].get_value<lob::blob_reference>(1);
        auto ref2_src = result[0].get_value<lob::clob_reference>(2);
        EXPECT_NE(ref1.object_id(), ref1_src.object_id());
        EXPECT_NE(ref2.object_id(), ref2_src.object_id());
        EXPECT_EQ(status::ok, tx->commit());
    }
}

TEST_F(blob_type_test, insert_from_select_duplication) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem handling two blob files of the same path";
    }
    // insert blob column from select, by duplicating src column into two dest columns
    // global::config_pool()->mock_datastore(true); // this test requires prod datastore
    datastore::get_datastore(true);
    global::config_pool()->enable_blob_cast(true);
    execute_statement("create table src (c0 int primary key, c1 blob, c2 clob)");
    execute_statement("insert into src values (1, CAST(x'000102' as BLOB), CAST('ABC' as CLOB))");
    execute_statement("create table dest (c0 int primary key, c10 blob, c11 blob, c20 clob, c21 clob)");

    execute_statement("INSERT INTO dest SELECT c0, c1, c1, c2, c2 from src");

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c10, c11, c20, c21 FROM dest", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref10 = result[0].get_value<lob::blob_reference>(1);
    auto ref11 = result[0].get_value<lob::blob_reference>(2);
    auto ref20 = result[0].get_value<lob::clob_reference>(3);
    auto ref21 = result[0].get_value<lob::clob_reference>(4);

    EXPECT_NE(ref10.object_id(), ref11.object_id());
    EXPECT_NE(ref20.object_id(), ref21.object_id());

    auto* ds = datastore::get_datastore(false);

    auto ret10 = ds->get_blob_file(ref10.object_id());
    ASSERT_TRUE(ret10);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret10.path().string())) << ret10.path().string();
    auto ret11 = ds->get_blob_file(ref11.object_id());
    ASSERT_TRUE(ret11);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret11.path().string())) << ret11.path().string();

    auto ret20 = ds->get_blob_file(ref20.object_id());
    ASSERT_TRUE(ret20);
    EXPECT_EQ("ABC", read_file(ret20.path().string())) << ret20.path().string();
    auto ret21 = ds->get_blob_file(ref21.object_id());
    ASSERT_TRUE(ret21);
    EXPECT_EQ("ABC", read_file(ret21.path().string())) << ret21.path().string();

    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::blob,
                                            kind::clob, kind::clob>({
                  1,
                  lob::blob_reference{ref10.object_id(),
                                      lob::lob_data_provider::datastore},
                  lob::blob_reference{ref11.object_id(),
                                      lob::lob_data_provider::datastore},
                  lob::clob_reference{ref20.object_id(),
                                      lob::lob_data_provider::datastore},
                  lob::clob_reference{ref21.object_id(),
                                      lob::lob_data_provider::datastore},
                  })),
              result[0]);
    EXPECT_EQ(status::ok, tx->commit());

    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM src", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1_src = result[0].get_value<lob::blob_reference>(1);
        auto ref2_src = result[0].get_value<lob::clob_reference>(2);
        EXPECT_NE(ref10.object_id(), ref1_src.object_id());
        EXPECT_NE(ref11.object_id(), ref1_src.object_id());
        EXPECT_NE(ref20.object_id(), ref2_src.object_id());
        EXPECT_NE(ref21.object_id(), ref2_src.object_id());
        EXPECT_EQ(status::ok, tx->commit());
    }
}

TEST_F(blob_type_test, insert_generated_blob) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(true);

    // currently mock is not supported
    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");

    execute_statement("INSERT INTO t VALUES (1, CAST(x'000102' as BLOB), CAST('ABC' as CLOB))");
    {
        // quickly verify the blobs by casting
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, CAST(c1 as varbinary), CAST(c2 as varchar) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::octet, kind::character>(
                      {1,  accessor::binary{"\x00\x01\x02"}, accessor::text{"ABC"}})),
                  result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(1);
        auto ref2 = result[0].get_value<lob::clob_reference>(2);

        auto* ds = datastore::get_datastore(false);
        auto ret1 = ds->get_blob_file(ref1.object_id());
        ASSERT_TRUE(ret1);
        EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1.path().string())) << ret1.path().string();
        auto ret2 = ds->get_blob_file(ref2.object_id());
        ASSERT_TRUE(ret2);
        EXPECT_EQ("ABC"sv, read_file(ret2.path().string())) << ret2.path().string();
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                      {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                       lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
                  result[0]);
    }
}

TEST_F(blob_type_test, update_generated_blob) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(true);

    // currently mock is not supported
    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");

    execute_statement("INSERT INTO t VALUES (1, CAST(x'000102' as BLOB), CAST('ABC' as CLOB))");
    execute_statement("UPDATE t SET c1=CAST(x'000102' as BLOB), c2 = CAST('ABC' as CLOB) WHERE c0 = 1");
    {
        // quickly verify the blobs by casting
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, CAST(c1 as varbinary), CAST(c2 as varchar) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::octet, kind::character>(
                      {1,  accessor::binary{"\x00\x01\x02"}, accessor::text{"ABC"}})),
                  result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(1);
        auto ref2 = result[0].get_value<lob::clob_reference>(2);

        auto* ds = datastore::get_datastore(false);
        auto ret1 = ds->get_blob_file(ref1.object_id());
        ASSERT_TRUE(ret1);
        EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1.path().string())) << ret1.path().string();
        auto ret2 = ds->get_blob_file(ref2.object_id());
        ASSERT_TRUE(ret2);
        EXPECT_EQ("ABC"sv, read_file(ret2.path().string())) << ret2.path().string();
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                      {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                       lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
                  result[0]);
        EXPECT_EQ(status::ok, tx->commit());
    }
}

TEST_F(blob_type_test, query_generated_blob) {
    // cast varbinary -> blob testing with query
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(true);

    // currently mock is not supported
    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 varbinary, c2 varchar)");
    execute_statement("INSERT INTO t VALUES (1, x'000102', 'ABC')");

    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, CAST(c1 as blob), CAST(c2 as clob) FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(1);
        auto ref2 = result[0].get_value<lob::clob_reference>(2);

        auto* ds = datastore::get_datastore(false);
        auto ret1 = ds->get_blob_file(ref1.object_id());
        ASSERT_TRUE(ret1);
        EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1.path().string())) << ret1.path().string();
        auto ret2 = ds->get_blob_file(ref2.object_id());
        ASSERT_TRUE(ret2);
        EXPECT_EQ("ABC"sv, read_file(ret2.path().string())) << ret2.path().string();
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                      {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                       lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
                  result[0]);
    }
}

TEST_F(blob_type_test, query_provided_blob) {
    // read blob input parameter with query without casting
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (1)");

    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "\x00\x01\x02"sv);
    create_file(path2, "ABC");

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT :p0, :p1, :p2 FROM t", variables, *ps, *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore();
    auto ret1 = ds->get_blob_file(ref1.object_id());
    ASSERT_TRUE(ret1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1.path().string())) << ret1.path().string();
    auto ret2 = ds->get_blob_file(ref2.object_id());
    ASSERT_TRUE(ret2);
    EXPECT_EQ("ABC", read_file(ret2.path().string())) << ret2.path().string();
    // currently input blobs are registered to datastore first so the provider is datastore
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);
    EXPECT_EQ(status::ok, tx->commit());
}

TEST_F(blob_type_test, query_provided_blob_by_casting) {
    // read blob input parameter with query with casting blob to varbinary
    global::config_pool()->enable_blob_cast(true);
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (1)");

    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "\x00\x01\x02"sv);
    create_file(path2, "ABC");

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    std::vector<mock::basic_record> result{};
    execute_query("SELECT :p0, cast(:p1 as varbinary), cast(:p2 as varchar) FROM t", variables, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::octet, kind::character>(
                  {1,  accessor::binary{"\x00\x01\x02"}, accessor::text{"ABC"}})),
              result[0]);
}

TEST_F(blob_type_test, insert_provided_blob_by_casting) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(true);

    // currently mock is not supported
    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);

    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "\x00\x01\x02"sv);
    create_file(path2, "ABC");

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    execute_statement("create table t (c0 int primary key, c1 varbinary, c2 varchar)");
    execute_statement("INSERT INTO t VALUES (:p0, CAST(:p1 as varbinary), CAST(:p2 as varchar))", variables, *ps);
    {
        // quickly verify the blobs by casting
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1, c2 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::octet, kind::character>(
                      {1,  accessor::binary{"\x00\x01\x02"}, accessor::text{"ABC"}})),
                  result[0]);
    }
}

TEST_F(blob_type_test, implicit_cast) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(true);

    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    execute_statement("INSERT INTO t VALUES (1, x'000102', 'ABC')");
    {
        // quickly verify the blobs by casting
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, CAST(c1 as varbinary), CAST(c2 as varchar) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::octet, kind::character>(
                      {1,  accessor::binary{"\x00\x01\x02"}, accessor::text{"ABC"}})),
                  result[0]);
    }
}

TEST_F(blob_type_test, insert_provided_multiple_times) {
    // insert the same provided blob multiple times
    // expecting same data with different object id
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "\x00\x01\x02"sv);
    create_file(path2, "ABC");

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    execute_statement("INSERT INTO t VALUES (1, :p1, :p2), (2, :p1, :p2)", variables, *ps);
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t ORDER BY c0", *tx, result);
    ASSERT_EQ(2, result.size());

    auto ref0_1 = result[0].get_value<lob::blob_reference>(1);
    auto ref0_2 = result[0].get_value<lob::clob_reference>(2);
    auto ref1_1 = result[1].get_value<lob::blob_reference>(1);
    auto ref1_2 = result[1].get_value<lob::clob_reference>(2);
    EXPECT_NE(ref0_1.object_id(), ref1_1.object_id());
    EXPECT_NE(ref0_2.object_id(), ref1_2.object_id());

    auto* ds = datastore::get_datastore();
    auto ret0_1 = ds->get_blob_file(ref0_1.object_id());
    ASSERT_TRUE(ret0_1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret0_1.path().string())) << ret0_1.path().string();
    auto ret0_2 = ds->get_blob_file(ref0_2.object_id());
    ASSERT_TRUE(ret0_2);
    EXPECT_EQ("ABC", read_file(ret0_2.path().string())) << ret0_2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref0_1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref0_2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);

    auto ret1_1 = ds->get_blob_file(ref1_1.object_id());
    ASSERT_TRUE(ret1_1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1_1.path().string())) << ret1_1.path().string();
    auto ret1_2 = ds->get_blob_file(ref1_2.object_id());
    ASSERT_TRUE(ret1_2);
    EXPECT_EQ("ABC", read_file(ret1_2.path().string())) << ret1_2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {2,  lob::blob_reference{ref1_1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref1_2.object_id(), lob::lob_data_provider::datastore}})),
              result[1]);
    EXPECT_EQ(status::ok, tx->commit());
}

//TODO enable the testcase when handling temporary blob is implemented (see issue #1114)
TEST_F(blob_type_test, DISABLED_insert_provided_multiple_times_is_temporary_true) {
    // same as insert_provided_multiple_times except the passed blob is temporary

    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "\x00\x01\x02"sv);
    create_file(path2, "ABC");

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1, true}); // is_temporary = true
    ps->set_clob("p2", lob::clob_locator{path2, true}); // is_temporary = true
    execute_statement("INSERT INTO t VALUES (1, :p1, :p2), (2, :p1, :p2)", variables, *ps);
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t ORDER BY c0", *tx, result);
    ASSERT_EQ(2, result.size());

    auto ref0_1 = result[0].get_value<lob::blob_reference>(1);
    auto ref0_2 = result[0].get_value<lob::clob_reference>(2);
    auto ref1_1 = result[1].get_value<lob::blob_reference>(1);
    auto ref1_2 = result[1].get_value<lob::clob_reference>(2);
    EXPECT_NE(ref0_1.object_id(), ref1_1.object_id());
    EXPECT_NE(ref0_2.object_id(), ref1_2.object_id());

    auto* ds = datastore::get_datastore();
    auto ret0_1 = ds->get_blob_file(ref0_1.object_id());
    ASSERT_TRUE(ret0_1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret0_1.path().string())) << ret0_1.path().string();
    auto ret0_2 = ds->get_blob_file(ref0_2.object_id());
    ASSERT_TRUE(ret0_2);
    EXPECT_EQ("ABC", read_file(ret0_2.path().string())) << ret0_2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref0_1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref0_2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);

    auto ret1_1 = ds->get_blob_file(ref1_1.object_id());
    ASSERT_TRUE(ret1_1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1_1.path().string())) << ret1_1.path().string();
    auto ret1_2 = ds->get_blob_file(ref1_2.object_id());
    ASSERT_TRUE(ret1_2);
    EXPECT_EQ("ABC", read_file(ret1_2.path().string())) << ret1_2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {2,  lob::blob_reference{ref1_1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref1_2.object_id(), lob::lob_data_provider::datastore}})),
              result[1]);
    EXPECT_EQ(status::ok, tx->commit());
}

TEST_F(blob_type_test, update_provided_multiple_times) {
    // update and assign the same provided blob for multiple records
    // expecting same data with different object id
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    execute_statement("insert into t values (1, null, null), (2, null, null)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    create_file(path1, "\x00\x01\x02"sv);
    create_file(path2, "ABC");

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    execute_statement("UPDATE t SET c1 = :p1, c2 = :p2", variables, *ps);
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t ORDER BY c0", *tx, result);
    ASSERT_EQ(2, result.size());

    auto ref0_1 = result[0].get_value<lob::blob_reference>(1);
    auto ref0_2 = result[0].get_value<lob::clob_reference>(2);
    auto ref1_1 = result[1].get_value<lob::blob_reference>(1);
    auto ref1_2 = result[1].get_value<lob::clob_reference>(2);
    EXPECT_NE(ref0_1.object_id(), ref1_1.object_id());
    EXPECT_NE(ref0_2.object_id(), ref1_2.object_id());

    auto* ds = datastore::get_datastore();
    auto ret0_1 = ds->get_blob_file(ref0_1.object_id());
    ASSERT_TRUE(ret0_1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret0_1.path().string())) << ret0_1.path().string();
    auto ret0_2 = ds->get_blob_file(ref0_2.object_id());
    ASSERT_TRUE(ret0_2);
    EXPECT_EQ("ABC", read_file(ret0_2.path().string())) << ret0_2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {1,  lob::blob_reference{ref0_1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref0_2.object_id(), lob::lob_data_provider::datastore}})),
              result[0]);

    auto ret1_1 = ds->get_blob_file(ref1_1.object_id());
    ASSERT_TRUE(ret1_1);
    EXPECT_EQ("\x00\x01\x02"sv, read_file(ret1_1.path().string())) << ret1_1.path().string();
    auto ret1_2 = ds->get_blob_file(ref1_2.object_id());
    ASSERT_TRUE(ret1_2);
    EXPECT_EQ("ABC", read_file(ret1_2.path().string())) << ret1_2.path().string();
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::blob, kind::clob>(
                  {2,  lob::blob_reference{ref1_1.object_id(), lob::lob_data_provider::datastore},
                   lob::clob_reference{ref1_2.object_id(), lob::lob_data_provider::datastore}})),
              result[1]);
    EXPECT_EQ(status::ok, tx->commit());
}

TEST_F(blob_type_test, insert_file_io_error) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    // verify limestone raises io error and it's handled correctly
    execute_statement("create table t (c0 int primary key, c1 blob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
        };

    auto path1 = path()+"/dummy_file.dat";
    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    test_stmt_err("INSERT INTO t VALUES (:p0, :p1)", variables, *ps, error_code::lob_file_io_error);
}

TEST_F(blob_type_test, read_file_error) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(true);
    // verify limestone raises error and it's handled correctly
    execute_statement("create table t (c0 int primary key, c1 blob)");
    execute_statement("INSERT INTO t VALUES (1, CAST(x'000102' as BLOB))");

    boost::filesystem::path path{};
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c1 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(0);

        auto* ds = datastore::get_datastore(false);
        auto ret1 = ds->get_blob_file(ref1.object_id());
        ASSERT_TRUE(ret1);
        EXPECT_EQ(status::ok, tx->commit());
        path = ret1.path();
    }
    // remove the blob file
    ASSERT_TRUE(boost::filesystem::remove(path));

    // limestone checks existence of file and if it does not exist, get_blob_file raises exception
    // we handle this situation as an invalid reference rather than io error
    test_stmt_err("SELECT CAST(c1 as varbinary) from t", error_code::lob_reference_invalid);
}

TEST_F(blob_type_test, cast_not_allowed_insert) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(false);

    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    test_stmt_err("INSERT INTO t VALUES (1, CAST(x'000102' as BLOB), CAST('ABC' as CLOB))", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("INSERT INTO t VALUES (1, x'000102', 'ABC')", error_code::unsupported_runtime_feature_exception);
}

TEST_F(blob_type_test, cast_not_allowed_update) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has to use mock and there is a problem generated blob for mock";
    }
    global::config_pool()->enable_blob_cast(true);

    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    execute_statement("INSERT INTO t VALUES (1, CAST(x'000102' as BLOB), CAST('ABC' as CLOB))");
    global::config_pool()->enable_blob_cast(false);
    test_stmt_err("UPDATE t SET c1=CAST(x'000102' as BLOB), c2 = CAST('ABC' as CLOB) WHERE c0 = 1", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("UPDATE t SET c1=x'000102', c2 ='ABC' WHERE c0 = 1", error_code::unsupported_runtime_feature_exception);
}

TEST_F(blob_type_test, invalid_types) {
    // without cast support, varbinary/varchar cannot be assigned to blob/clob columns
    global::config_pool()->enable_blob_cast(false);

    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob)");
    test_stmt_err("INSERT INTO t VALUES (1, x'000102')", error_code::unsupported_runtime_feature_exception);
    execute_statement("drop table t");
    execute_statement("create table t (c0 int primary key, c1 clob)");
    test_stmt_err("INSERT INTO t VALUES (1, 'ABC')", error_code::unsupported_runtime_feature_exception);
}

// FIXME fix the problem of invalid parameter types
TEST_F(blob_type_test, DISABLED_invalid_parameter_types) {
    // valid type used by host variables (placeholders), but invalid type is used for parameters
    // global::config_pool()->mock_datastore(true);
    // datastore::get_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
        };

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_octet("p1", "\x00\x01\x02"sv);

    test_stmt_err("INSERT INTO t VALUES (:p0, :p1)", variables, *ps, error_code::unsupported_runtime_feature_exception);
}

TEST_F(blob_type_test, max_len_to_cast_to_string) {
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    global::config_pool()->enable_blob_cast(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    std::string oct_content( octet_type_max_length_for_value, '\x01');
    std::string char_content(character_type_max_length_for_value, 'x');
    create_file(path1, oct_content);
    create_file(path2, char_content);

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    execute_statement("INSERT INTO t VALUES (:p0, :p1, :p2)", variables, *ps);

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, cast(c1 as varbinary), cast(c2 as varchar) FROM t", *tx, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::octet, kind::character>(
                  {1,  accessor::binary{oct_content}, accessor::text{char_content}})),
              result[0]);
}

TEST_F(blob_type_test, over_max_len_to_cast_to_string) {
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    global::config_pool()->enable_blob_cast(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    std::string oct_content( octet_type_max_length_for_value+1, '\x01');
    std::string char_content(character_type_max_length_for_value+1, 'x');
    create_file(path1, oct_content);
    create_file(path2, char_content);

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    execute_statement("INSERT INTO t VALUES (:p0, :p1, :p2)", variables, *ps);

    test_stmt_err("SELECT c0, cast(c1 as varbinary) FROM t", error_code::value_too_long_exception);
    test_stmt_err("SELECT c0, cast(c2 as varchar) FROM t", error_code::value_too_long_exception);
}

TEST_F(blob_type_test, empty_blobs_cast) {
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true);
    global::config_pool()->enable_blob_cast(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::blob},
            {"p2", api::field_type_kind::clob},
        };

    auto path1 = path()+"/file1.dat";
    auto path2 = path()+"/file2.dat";
    std::string oct_content{};
    std::string char_content{};
    create_file(path1, oct_content);
    create_file(path2, char_content);

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_blob("p1", lob::blob_locator{path1});
    ps->set_clob("p2", lob::clob_locator{path2});
    execute_statement("INSERT INTO t VALUES (:p0, :p1, :p2)", variables, *ps);

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, cast(c1 as varbinary), cast(c2 as varchar) FROM t", *tx, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::octet, kind::character>(
                  {1,  accessor::binary{oct_content}, accessor::text{char_content}})),
              result[0]);
}

}
