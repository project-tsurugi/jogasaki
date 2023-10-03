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
#include <jogasaki/executor/process/impl/ops/index_field_mapper.h>

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/data/any.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/executor/process/impl/ops/details/encode_key.h>
#include "api_test_base.h"
#include "../kvs_test_utils.h"
#include "jogasaki/utils/coder.h"

namespace jogasaki::api::impl {

using namespace std::literals::string_literals;
using namespace std::string_view_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;
using namespace jogasaki::executor::process::impl::ops;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using meta::field_enum_tag_t;
using kvs::end_point_kind;
using kind = meta::field_type_kind;

class secondary_index_dml_test :
    public ::testing::Test,
    public kvs_test_utils,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    std::unordered_set<std::int32_t> get_secondary_entries(std::string_view index_name, std::optional<std::int32_t> c1);
};


bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

using k = meta::field_type_kind;

TEST_F(secondary_index_dml_test, basic) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    {
        auto m = get_secondary_entries("I", 10);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
}

TEST_F(secondary_index_dml_test, insert_multiple_recs) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("INSERT INTO T VALUES(2,20,200)");
    execute_statement("INSERT INTO T VALUES(3,30,300)");
    auto m = get_secondary_entries("I", 20);
    ASSERT_EQ(1, m.size());
    EXPECT_EQ(1, m.count(2));
}

TEST_F(secondary_index_dml_test, insert_multiple_recs_for_same_index_key) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T VALUES(0,0,0)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("INSERT INTO T VALUES(2,10,200)");
    execute_statement("INSERT INTO T VALUES(3,10,300)");
    execute_statement("INSERT INTO T VALUES(4,20,400)");
    auto m = get_secondary_entries("I", 10);
    ASSERT_EQ(3, m.size());
    EXPECT_EQ(1, m.count(1));
    EXPECT_EQ(1, m.count(2));
    EXPECT_EQ(1, m.count(3));
}

TEST_F(secondary_index_dml_test, insert_null_in_secondary_index_key) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0) VALUES(0)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1, 10)");
    execute_statement("INSERT INTO T (C0) VALUES(2)");
    auto m = get_secondary_entries("I", {});
    ASSERT_EQ(2, m.size());
    EXPECT_EQ(1, m.count(0));
    EXPECT_EQ(1, m.count(2));
}

TEST_F(secondary_index_dml_test, delete) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("INSERT INTO T VALUES(2,20,200)");
    {
        auto m = get_secondary_entries("I", 10);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
    {
        auto m = get_secondary_entries("I", 20);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(2));
    }
    execute_statement("DELETE FROM T WHERE C0=2");
    {
        auto m = get_secondary_entries("I", 10);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
    {
        auto m = get_secondary_entries("I", 20);
        ASSERT_TRUE(m.empty());
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T WHERE C1=20", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(secondary_index_dml_test, update_pk) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("INSERT INTO T VALUES(2,20,200)");
    execute_statement("UPDATE T SET C0=3 WHERE C0=1");
    {
        auto m = get_secondary_entries("I", 10);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(3));
    }
    {
        auto m = get_secondary_entries("I", 20);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(2));
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T WHERE C1=10", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3,10)), result[0]);
}

TEST_F(secondary_index_dml_test, update_index_key) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("INSERT INTO T VALUES(2,20,200)");
    execute_statement("UPDATE T SET C1=30 WHERE C0=1");
    {
        auto m = get_secondary_entries("I", 30);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
    {
        auto m = get_secondary_entries("I", 20);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(2));
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T WHERE C1=30", result);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>({1,30})), result[0]);
}

TEST_F(secondary_index_dml_test, update_nonpk_non_index_key_column) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("INSERT INTO T VALUES(2,20,200)");
    execute_statement("UPDATE T SET C2=300 WHERE C0=1");
    {
        auto m = get_secondary_entries("I", 10);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
    {
        auto m = get_secondary_entries("I", 20);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(2));
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T WHERE C1=10", result);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>({1,10,300})), result[0]);
}

TEST_F(secondary_index_dml_test, insert_multi_secondaries) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1)");
    execute_statement("CREATE INDEX I2 ON T (C2)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    {
        auto m = get_secondary_entries("I1", 10);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
    {
        auto m = get_secondary_entries("I2", 100);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
}

TEST_F(secondary_index_dml_test, delete_multi_secondaries) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1)");
    execute_statement("CREATE INDEX I2 ON T (C2)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("DELETE FROM T WHERE C0=1");
    {
        auto m = get_secondary_entries("I1", 10);
        ASSERT_TRUE(m.empty());
    }
    {
        auto m = get_secondary_entries("I2", 100);
        ASSERT_TRUE(m.empty());
    }
}

TEST_F(secondary_index_dml_test, update_pk_multi_secondaries) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1)");
    execute_statement("CREATE INDEX I2 ON T (C2)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("UPDATE T SET C0=2 WHERE C0=1");
    {
        auto m = get_secondary_entries("I1", 10);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(2));
    }
    {
        auto m = get_secondary_entries("I2", 100);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(2));
    }
}

TEST_F(secondary_index_dml_test, update_index_key_multi_secondaries) {
    // update only I1 key, not affecting I2
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1)");
    execute_statement("CREATE INDEX I2 ON T (C2)");
    execute_statement("INSERT INTO T VALUES(1,10,100)");
    execute_statement("UPDATE T SET C1=20 WHERE C0=1");
    {
        auto m = get_secondary_entries("I1", 20);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
    {
        auto m = get_secondary_entries("I2", 100);
        ASSERT_EQ(1, m.size());
        EXPECT_EQ(1, m.count(1));
    }
}

std::unordered_set<std::int32_t> secondary_index_dml_test::get_secondary_entries(std::string_view index_name, std::optional<std::int32_t> secondary_key) {
    auto table = get_impl(*db_).kvs_db()->get_storage("T");
    auto index = get_impl(*db_).kvs_db()->get_storage(index_name);
    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    {
        basic_record result{create_nullable_record<k::int4, k::int4, k::int4>(0, 0, 0)};
        index_field_mapper mapper{
            {
                {
                    meta::field_type{field_enum_tag_t<k::int4>{}},
                    true,
                    result.record_meta()->value_offset(0),
                    result.record_meta()->nullity_offset(0),
                    false,
                    kvs::spec_key_ascending
                },
            },
            {
                {
                    meta::field_type{field_enum_tag_t<k::int4>{}},
                    true,
                    result.record_meta()->value_offset(1),
                    result.record_meta()->nullity_offset(1),
                    true,
                    kvs::spec_value
                },
                {
                    meta::field_type{field_enum_tag_t<k::int4>{}},
                    true,
                    result.record_meta()->value_offset(2),
                    result.record_meta()->nullity_offset(2),
                    true,
                    kvs::spec_value
                },
            },
            {
                {
                    meta::field_type{field_enum_tag_t<k::int4>{}},
                    true,
                    kvs::spec_key_ascending
                },
            },
        };
        std::unordered_set<std::int32_t> ret{};
        {
            data::aligned_buffer buf{};
            auto v = secondary_key.has_value() ? data::any{std::in_place_type<std::int32_t>, *secondary_key} : data::any{};

            if(auto res = utils::encode_any(
                    buf,
                    meta::field_type{field_enum_tag_t<k::int4>{}},
                    true,
                    kvs::spec_key_ascending,
                    {v}
                ); res != status::ok) {
                fail();
            }

            auto tx = wrap(get_impl(*db_).kvs_db()->create_transaction());
            std::unique_ptr<kvs::iterator> it{};
            if(status::ok != index->scan(*tx, buf, end_point_kind::prefixed_inclusive, buf, end_point_kind::prefixed_inclusive, it)) {
                fail();
            };
            while(status::ok == it->next()) {
                std::string_view key{};
                std::string_view value{};
                if(status::ok != it->key(key)) {
                    fail();
                };
                if(status::ok != it->value(value)) {
                    fail();
                }
                if(status::ok != mapper(key, value, result.ref(), *table, *tx, &resource)) {
                    fail();
                }
                ret.emplace(
                    result.ref().get_value<std::int32_t>(result.record_meta()->value_offset(0))
                );
            }
            it.reset();
            if(status::ok != tx->commit()) {
                fail();
            }
        }
        return ret;
    }
}

}
