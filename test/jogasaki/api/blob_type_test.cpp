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
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;
using kind = meta::field_type_kind;

TEST_F(blob_type_test, insert) {
    global::config_pool()->mock_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::blob},
        {"p2", api::field_type_kind::clob},
    };

    auto path1 = path()+"/blob_types1.dat";
    auto path2 = path()+"/blob_types2.dat";
    create_file(path1, "ABC");
    create_file(path2, "DEF");

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

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), false);
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
}

TEST_F(blob_type_test, blob_pool_release) {
    // verify blob pool is correctly released when transaction completes
    global::config_pool()->mock_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::blob},
    };

    auto path1 = path()+"/blob_types1.dat";
    create_file(path1, "ABC");

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
    // global::config_pool()->mock_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::blob},
        {"p2", api::field_type_kind::clob},
    };

    auto path1 = path()+"/blob_types1.dat";
    auto path2 = path()+"/blob_types2.dat";
    create_file(path1, "ABC");
    create_file(path2, "DEF");

    auto path3 = path()+"/blob_types3.dat";
    auto path4 = path()+"/blob_types4.dat";
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

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), false);
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
    // global::config_pool()->mock_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::blob},
        {"p2", api::field_type_kind::clob},
    };

    auto path1 = path()+"/blob_types1.dat";
    auto path2 = path()+"/blob_types2.dat";
    create_file(path1, "ABC");
    create_file(path2, "DEF");

    auto path3 = path()+"/blob_types3.dat";
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

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), false);
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
    // update some blob column while keeping the other unchanged
    // global::config_pool()->mock_datastore(true);
    execute_statement("create table src (c0 int primary key, c1 blob, c2 clob)");
    execute_statement("create table dest (c0 int primary key, c1 blob, c2 clob)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::blob},
        {"p2", api::field_type_kind::clob},
    };

    auto path1 = path()+"/blob_types1.dat";
    auto path2 = path()+"/blob_types2.dat";
    create_file(path1, "ABC");
    create_file(path2, "DEF");

    auto path3 = path()+"/blob_types3.dat";
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

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), false);
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

// cast with blob not yet supported
TEST_F(blob_type_test, DISABLED_insert_generated_blob) {
    global::config_pool()->mock_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");

    execute_statement("INSERT INTO t VALUES (0, CAST(CAST('000102' as varbinary) as BLOB), CAST(CAST('ABC' as varchar) as CLOB))");
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), false);
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
}

}
