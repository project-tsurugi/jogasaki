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
#include <jogasaki/api.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <takatori/type/int.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/api/field_type_kind.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;

using namespace yugawara::storage;

namespace type = takatori::type;
using nullity = yugawara::variable::nullity;

/**
 * @brief test database api
 */
class metadata_test : public ::testing::Test {

};

TEST_F(metadata_test, create_table_with_primary_index_dynamic) {
    auto db = api::create_database();
    db->start();

    takatori::util::reference_vector<yugawara::storage::column> columns;
    columns.emplace_back(yugawara::storage::column("C0", takatori::type::int8(), yugawara::variable::nullity(false)));
    columns.emplace_back(yugawara::storage::column("C1", takatori::type::float8(), yugawara::variable::nullity(true)));

    auto t = std::make_shared<table>(
        "TEST", columns);
    ASSERT_EQ(status::ok, db->create_table(t));

    std::vector<yugawara::storage::index::key> keys;
    keys.emplace_back(yugawara::storage::index::key(columns[0], takatori::relation::sort_direction::ascendant));

    std::vector<yugawara::storage::index::column_ref> values;
    values.emplace_back(yugawara::storage::index::column_ref(columns[1]));

    auto i = std::make_shared<yugawara::storage::index>(
        t,
        "TEST",
        keys,
        values,
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db->create_index(i));
    {
        std::unique_ptr<api::executable_statement> exec{};
        auto tx = db->create_transaction();
        ASSERT_EQ(status::ok,db->create_executable("INSERT INTO TEST (C0, C1) VALUES(0, 1.0)", exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    {
        auto tx = db->create_transaction();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db->create_executable("select * from TEST order by C0", exec));
        db->explain(*exec, std::cout);
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok,tx->execute(*exec, rs));
        auto it = rs->iterator();
        std::size_t count = 0;
        while(it->has_next()) {
            std::stringstream ss{};
            auto* record = it->next();
            ss << *record;
            LOG(INFO) << ss.str();
            ++count;
        }
        EXPECT_EQ(1, count);
        tx->commit();
    }
    db->stop();
}

TEST_F(metadata_test, create_table_with_primary_index) {
    auto db = api::create_database();
    db->start();
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "C1", type::float8 (), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        "TEST",
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db->create_index(i));
    {
        std::unique_ptr<api::executable_statement> exec{};
        auto tx = db->create_transaction();
        ASSERT_EQ(status::ok,db->create_executable("INSERT INTO TEST (C0, C1) VALUES(0, 1.0)", exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    {
        auto tx = db->create_transaction();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db->create_executable("select * from TEST order by C0", exec));
        db->explain(*exec, std::cout);
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok,tx->execute(*exec, rs));
        auto it = rs->iterator();
        std::size_t count = 0;
        while(it->has_next()) {
            std::stringstream ss{};
            auto* record = it->next();
            ss << *record;
            LOG(INFO) << ss.str();
            ++count;
        }
        EXPECT_EQ(1, count);
        tx->commit();
    }
    db->stop();
}

// disable until secondary index is ready
TEST_F(metadata_test, DISABLED_create_table_with_secondary_index) {
    auto db = api::create_database();
    db->start();
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "C1", type::float8 (), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        "TEST",
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db->create_index(i));
    auto i2 = std::make_shared<yugawara::storage::index>(
        t,
        "TEST_SECONDARY",
        std::initializer_list<index::key>{
            t->columns()[1],
        },
        std::initializer_list<index::column_ref>{
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
        }
    );
    ASSERT_EQ(status::ok, db->create_index(i2));
    {
        std::unique_ptr<api::executable_statement> exec{};
        auto tx = db->create_transaction();
        ASSERT_EQ(status::ok,db->create_executable("INSERT INTO TEST (C0, C1) VALUES(0, 1.0)", exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    {
        auto tx = db->create_transaction();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db->create_executable("select * from TEST where C1=1.0", exec));
        db->explain(*exec, std::cout);
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok,tx->execute(*exec, rs));
        auto it = rs->iterator();
        std::size_t count = 0;
        while(it->has_next()) {
            std::stringstream ss{};
            auto* record = it->next();
            ss << *record;
            LOG(INFO) << ss.str();
            ++count;
        }
        EXPECT_EQ(1, count);
        tx->commit();
    }
    db->stop();
}

TEST_F(metadata_test, crud1) {
    auto db = api::create_database();
    db->start();
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "C1", type::float8 (), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db->create_table(t));
    ASSERT_EQ(status::err_already_exists, db->create_table(t));
    EXPECT_EQ(t, db->find_table(t->simple_name()));
    EXPECT_FALSE(db->find_table("dummy"));

    auto i = std::make_shared<yugawara::storage::index>(
        t,
        "TEST",
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db->create_index(i));
    ASSERT_EQ(status::err_already_exists, db->create_index(i));
    EXPECT_EQ(i, db->find_index(i->simple_name()));
    EXPECT_FALSE(db->find_index("dummy"));

    ASSERT_EQ(status::ok, db->drop_index(i->simple_name()));
    ASSERT_EQ(status::not_found, db->drop_index(i->simple_name()));

    ASSERT_EQ(status::ok, db->drop_table(t->simple_name()));
    ASSERT_EQ(status::not_found, db->drop_table(t->simple_name()));

    auto seq = std::make_shared<sequence>(
        100,
        "SEQ"
    );
    ASSERT_EQ(status::ok, db->create_sequence(seq));
    ASSERT_EQ(status::err_already_exists, db->create_sequence(seq));
    EXPECT_EQ(seq, db->find_sequence(seq->simple_name()));
    EXPECT_FALSE(db->find_sequence("dummy"));
    ASSERT_EQ(status::ok, db->drop_sequence(seq->simple_name()));
    ASSERT_EQ(status::not_found, db->drop_sequence(seq->simple_name()));

    db->stop();
}

TEST_F(metadata_test, use_sequence) {
    auto db = api::create_database();
    db->start();
    auto seq = std::make_shared<sequence>(
        100,
        "SEQ"
    );
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false}, column_value{seq}},
            column{ "C1", type::float8 (), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db->create_table(t));
    db->stop();
}
}
