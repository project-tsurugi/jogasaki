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
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/type/type_kind.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/variable/nullity.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/status.h>
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
