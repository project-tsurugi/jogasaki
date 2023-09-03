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

    std::unordered_map<std::int32_t, std::int32_t> get_secondary_entries(std::string_view index_name, std::optional<std::int32_t> c1);
};


bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

using k = meta::field_type_kind;

TEST_F(secondary_index_dml_test, basic) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,10)");
    auto m = get_secondary_entries("I", 10);
    ASSERT_EQ(1, m.size());
    EXPECT_EQ(10, m.at(1));
}

TEST_F(secondary_index_dml_test, multiple_recs) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,10)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(2,20)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(3,30)");
    auto m = get_secondary_entries("I", 20);
    ASSERT_EQ(1, m.size());
    EXPECT_EQ(20, m.at(2));
}

TEST_F(secondary_index_dml_test, multiple_recs_for_same_index_key) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(0,10)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,20)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(2,20)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(3,20)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(4,30)");
    auto m = get_secondary_entries("I", 20);
    ASSERT_EQ(3, m.size());
    EXPECT_EQ(20, m.at(1));
    EXPECT_EQ(20, m.at(2));
    EXPECT_EQ(20, m.at(3));
}

std::unordered_map<std::int32_t, std::int32_t> secondary_index_dml_test::get_secondary_entries(std::string_view index_name, std::optional<std::int32_t> c1) {
    auto table = get_impl(*db_).kvs_db()->get_storage("T");
    auto index = get_impl(*db_).kvs_db()->get_storage(index_name);
    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    {
        basic_record result{create_nullable_record<k::int4, k::int4>(0, 0)};
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
            },
            {
                {
                    meta::field_type{field_enum_tag_t<k::int4>{}},
                    true,
                    kvs::spec_key_ascending
                },
            },
        };
        std::unordered_map<std::int32_t, std::int32_t> map{};
        {

            data::aligned_buffer buf{};
            if(auto res = utils::encode_any(
                    buf,
                    meta::field_type{field_enum_tag_t<k::int4>{}},
                    true,
                    kvs::spec_key_ascending,
                    {
                        data::any{std::in_place_type<std::int32_t>, *c1}
                    }
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
                map.emplace(
                    result.ref().get_value<std::int32_t>(result.record_meta()->value_offset(0)),
                    result.ref().get_value<std::int32_t>(result.record_meta()->value_offset(1))
                );
            }
            it.reset();
            if(status::ok != tx->commit()) {
                fail();
            }
        }
        return map;
    }
}

}
