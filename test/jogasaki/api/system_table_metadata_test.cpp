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
#include <takatori/type/octet.h>
#include <takatori/type/bit.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/proto_debug_string.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;

using namespace yugawara::storage;

namespace type = takatori::type;
using nullity = yugawara::variable::nullity;
using api::impl::get_impl;

/**
 * @brief test metadata on system built-in table
 */
class system_table_metadata_test :
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

TEST_F(system_table_metadata_test, create_table_with_primary_index) {
    // simply start db and check if system table has metadata
    verify_index_storage_metadata("__system_sequences");
}

}
