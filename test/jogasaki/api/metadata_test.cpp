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
#include <initializer_list>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <takatori/type/bit.h>
#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/column_value.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/record.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/proto_debug_string.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;

using namespace yugawara::storage;
using index = yugawara::storage::index;

namespace type = takatori::type;
using nullity = yugawara::variable::nullity;
using api::impl::get_impl;

/**
 * @brief test database api
 */
class metadata_test :
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
    }

    void TearDown() override {
        db_teardown();
    }

    void test_unsupported_column_type(type::data&& typ);

    void verify_index_storage_metadata(std::string_view name) {
        // synthesized flag is not in yugawara config. provider, so check manually
        auto kvs = get_impl(*db_).kvs_db();
        auto stg = kvs->get_storage(name);
        ASSERT_TRUE(stg);
        sharksfin::StorageOptions options{};
        ASSERT_EQ(status::ok, stg->get_options(options));
        auto src = options.payload();
        proto::metadata::storage::Storage storage{};
        if (! storage.ParseFromArray(src.data(), static_cast<int>(src.size()))) {
            FAIL();
        }
        std::cerr << "storage_option_json:" << utils::to_debug_string(storage) << std::endl;
        ASSERT_TRUE(storage.index().synthesized());
    }
};

TEST_F(metadata_test, create_table_with_primary_index) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "C1", type::float8(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
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
    ASSERT_EQ(status::ok, db_->create_index(i));
    verify_index_storage_metadata("TEST");
    {
        auto tx = utils::create_transaction(*db_);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->create_executable("INSERT INTO TEST (C0, C1) VALUES(0, 1.0)", exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    {
        auto tx = utils::create_transaction(*db_);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->create_executable("select * from TEST order by C0", exec));
//        db_->explain(*exec, std::cout);
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
}

TEST_F(metadata_test, primary_index_with_nullable_columns) {
    // primary key column must not be nullable
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int4(), nullity{true} },
            column{ "C1", type::int4(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
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
    ASSERT_EQ(status::err_illegal_operation, db_->create_index(i));
}

TEST_F(metadata_test, create_table_with_secondary_index) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "C1", type::float8(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
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
    ASSERT_EQ(status::ok, db_->create_index(i));
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
    ASSERT_EQ(status::ok, db_->create_index(i2));
    verify_index_storage_metadata("TEST_SECONDARY");
    {
        auto tx = utils::create_transaction(*db_);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->create_executable("INSERT INTO TEST (C0, C1) VALUES(0, 1.0)", exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    {
        auto tx = utils::create_transaction(*db_);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->create_executable("select * from TEST where C1=1.0", exec));
//        db_->explain(*exec, std::cout);
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
}

TEST_F(metadata_test, crud1) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "C1", type::float8(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    ASSERT_EQ(status::err_already_exists, db_->create_table(t));
    EXPECT_EQ(t, db_->find_table(t->simple_name()));
    EXPECT_FALSE(db_->find_table("dummy"));

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
    ASSERT_EQ(status::ok, db_->create_index(i));
    ASSERT_EQ(status::err_already_exists, db_->create_index(i));
    ASSERT_TRUE(db_->find_index(i->simple_name()));
    EXPECT_NE(i, db_->find_index(i->simple_name()));  // create_index serialize/deserialize input, so not equals
    EXPECT_FALSE(db_->find_index("dummy"));

    ASSERT_EQ(status::ok, db_->drop_index(i->simple_name()));
    ASSERT_EQ(status::not_found, db_->drop_index(i->simple_name()));

    ASSERT_EQ(status::ok, db_->drop_table(t->simple_name()));
    ASSERT_EQ(status::not_found, db_->drop_table(t->simple_name()));

    auto seq = std::make_shared<yugawara::storage::sequence>(
        100,
        "SEQ"
    );
    ASSERT_EQ(status::ok, db_->create_sequence(seq));
    ASSERT_EQ(status::err_already_exists, db_->create_sequence(seq));
    EXPECT_EQ(seq, db_->find_sequence(seq->simple_name()));
    EXPECT_FALSE(db_->find_sequence("dummy"));
    ASSERT_EQ(status::ok, db_->drop_sequence(seq->simple_name()));
    ASSERT_EQ(status::not_found, db_->drop_sequence(seq->simple_name()));
}

TEST_F(metadata_test, use_sequence) {
    auto seq = std::make_shared<yugawara::storage::sequence>(
        100,
        "SEQ"
    );
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false}, column_value{seq}},
            column{ "C1", type::float8(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
}

void metadata_test::test_unsupported_column_type(type::data&& typ) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int4(), nullity{false} },
            column{ "C1", std::move(typ), nullity{true} },
        }
    );
    ASSERT_EQ(status::err_unsupported, db_->create_table(t));
}

TEST_F(metadata_test, unsupported_column_types) {
    test_unsupported_column_type(type::octet(10));
    test_unsupported_column_type(type::octet(type::varying, 10));
    test_unsupported_column_type(type::bit(10));
    test_unsupported_column_type(type::decimal(39));
    test_unsupported_column_type(type::decimal(0));
    test_unsupported_column_type(type::decimal(3, 4));
    test_unsupported_column_type(type::decimal({}, {}));
    test_unsupported_column_type(type::decimal(5, {}));
    test_unsupported_column_type(type::character(0));
    test_unsupported_column_type(type::character(type::varying, 0));
    test_unsupported_column_type(type::character(30717));
    test_unsupported_column_type(type::character(type::varying, 30717));
}


}
