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
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <gtest/gtest.h>

#include <takatori/type/type_kind.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/configuration.h>
#include <jogasaki/datastore/datastore.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/create_file.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using namespace yugawara;
using namespace yugawara::storage;

namespace type = takatori::type;
using nullity = yugawara::variable::nullity;

using kind = jogasaki::meta::field_type_kind;
/**
 * @brief test database recovery with blob
 */
class recovery_blob_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        datastore::get_datastore(db_impl()->kvs_db().get(), true);  // reset cache for datastore object as db setup recreates it
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(recovery_blob_test, basic) {
    // verify registered blob files remain and datastore provide blob_file correctly
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
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

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), true); // reset cache because ds instance is recreated
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

// FIXME uncomment when this is fixed
TEST_F(recovery_blob_test, DISABLED_update) {
    // verify old lob id is not usable any more
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
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

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_blob("p1", lob::blob_locator{path1});
        ps->set_clob("p2", lob::clob_locator{path2});
        execute_statement("INSERT INTO t VALUES (:p0, :p1, :p2)", variables, *ps);
    }
    lob::lob_id_type old_id1;
    lob::lob_id_type old_id2;
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(1);
        old_id1 = ref1.object_id();
        auto ref2 = result[0].get_value<lob::clob_reference>(2);
        old_id2 = ref2.object_id();
        EXPECT_EQ(status::ok, tx->commit());
    }
    auto path3 = path()+"/blob_types3.dat";
    create_file(path3, "abc");
    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_blob("p1", lob::blob_locator{path3});
        execute_statement("UPDATE t SET c1 = :p1 WHERE c0 = :p0", variables, *ps);
    }
    lob::lob_id_type new_id1;
    lob::lob_id_type new_id2;
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(1);
        new_id1 = ref1.object_id();
        auto ref2 = result[0].get_value<lob::clob_reference>(2);
        new_id2 = ref2.object_id();
        EXPECT_EQ(status::ok, tx->commit());
    }
    EXPECT_NE(new_id1, old_id1);
    EXPECT_NE(new_id2, old_id2);

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), true); // reset cache because ds instance is recreated
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

    EXPECT_NE(old_id1, ref1.object_id());
    EXPECT_NE(old_id2, ref2.object_id());
    auto old_ret1 = ds->get_blob_file(old_id1);
    ASSERT_FALSE(old_ret1);
    auto old_ret2 = ds->get_blob_file(old_id2);
    ASSERT_FALSE(old_ret2);

    EXPECT_EQ(status::ok, tx->commit());

}

TEST_F(recovery_blob_test, update_with_cast) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    // do same as update testcase, but by cast expression
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    global::config_pool()->enable_blob_cast(true);

    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    execute_statement("INSERT INTO t VALUES (1, CAST(CAST('414243' as varbinary) as blob), CAST('DEF' as clob))");
    lob::lob_id_type old_id1;
    lob::lob_id_type old_id2;
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(1);
        old_id1 = ref1.object_id();
        auto ref2 = result[0].get_value<lob::clob_reference>(2);
        old_id2 = ref2.object_id();
        EXPECT_EQ(status::ok, tx->commit());
    }
    execute_statement("UPDATE t SET c1 = CAST(CAST('616263' as varbinary) as blob) WHERE c0 = 1");
    lob::lob_id_type new_id1;
    lob::lob_id_type new_id2;
    {
        std::vector<mock::basic_record> result{};
        auto tx = utils::create_transaction(*db_);
        execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
        ASSERT_EQ(1, result.size());

        auto ref1 = result[0].get_value<lob::blob_reference>(1);
        new_id1 = ref1.object_id();
        auto ref2 = result[0].get_value<lob::clob_reference>(2);
        new_id2 = ref2.object_id();
        EXPECT_EQ(status::ok, tx->commit());
    }
    EXPECT_NE(new_id1, old_id1);
    EXPECT_NE(new_id2, old_id2);

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_);
    execute_query("SELECT c0, c1, c2 FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    auto ref1 = result[0].get_value<lob::blob_reference>(1);
    auto ref2 = result[0].get_value<lob::clob_reference>(2);

    auto* ds = datastore::get_datastore(db_impl()->kvs_db().get(), true); // reset cache because ds instance is recreated
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

    EXPECT_NE(old_id1, ref1.object_id());
    EXPECT_NE(old_id2, ref2.object_id());

    // maybe old id is not usable any more
    auto old_ret1 = ds->get_blob_file(old_id1);
    ASSERT_FALSE(old_ret1);
    auto old_ret2 = ds->get_blob_file(old_id2);
    ASSERT_FALSE(old_ret2);

    EXPECT_EQ(status::ok, tx->commit());

}

}
