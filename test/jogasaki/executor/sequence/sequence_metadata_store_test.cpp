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
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>
#include <yugawara/storage/sequence.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/system_storage.h>
#include <jogasaki/kvs_test_base.h>

namespace jogasaki::executor::sequence {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace executor;
using namespace takatori::util;

using namespace yugawara;
using namespace yugawara::storage;

class sequence_metadata_store_test :
    public ::testing::Test,
    public kvs_test_base {
public:
    void SetUp() override {
        kvs_db_setup();
        kvs::setup_system_storage();
    }
    void TearDown() override {
        kvs_db_teardown();
    }
};

TEST_F(sequence_metadata_store_test, simple) {
    auto tx = db_->create_transaction();
    metadata_store s{*tx};
    s.put(0, 0);
    s.put(1, 100);
    s.put(2, 200);
}

TEST_F(sequence_metadata_store_test, scan) {
    auto tx = db_->create_transaction();
    metadata_store s{*tx};
    s.put(1, 100);
    s.put(0, 0);
    s.put(2, 200);

    std::vector<std::pair<std::size_t, std::size_t>> result{};
    std::vector<std::pair<std::size_t, std::size_t>> exp{
        {0, 0},
        {1, 100},
        {2, 200},
    };
    s.scan([&] (std::size_t def_id, std::size_t id) {
        result.emplace_back(def_id, id);
    });
    EXPECT_EQ(exp, result);
}

std::size_t next_empty_slot(metadata_store& s) {
    std::size_t def_id{};
    s.find_next_empty_def_id(def_id);
    return def_id;
}

TEST_F(sequence_metadata_store_test, find_next_defid) {
    auto tx = db_->create_transaction();
    metadata_store s{*tx};
    EXPECT_EQ(0, next_empty_slot(s));
    s.put(1, 100);
    EXPECT_EQ(0, next_empty_slot(s));
    s.put(0, 0);
    EXPECT_EQ(2, next_empty_slot(s));
    s.put(2, 200);
    s.put(4, 200);
    EXPECT_EQ(3, next_empty_slot(s));
    s.put(3, 200);
    EXPECT_EQ(5, next_empty_slot(s));
}

TEST_F(sequence_metadata_store_test, remove) {
    auto tx = db_->create_transaction();
    metadata_store s{*tx};
    s.put(0, 0);
    s.put(1, 100);
    s.put(2, 200);
    EXPECT_EQ(3, s.size());
    EXPECT_TRUE(s.remove(1));
    EXPECT_EQ(2, s.size());
    EXPECT_TRUE(s.remove(2));
    EXPECT_EQ(1, s.size());
    EXPECT_FALSE(s.remove(3));
    EXPECT_EQ(1, s.size());
}

}

