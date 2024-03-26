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
#include <cstdint>
#include <iostream>
#include <memory>
#include <string_view>
#include <type_traits>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/kvs_test_utils.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_dump_formatter.h>

#include "../api/api_test_base.h"

namespace jogasaki::utils {

using namespace std::string_view_literals;

class storage_dump_formatter_test :
    public ::testing::Test,
    public testing::api_test_base,
    public kvs_test_utils
{
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        auto* impl = db_impl();
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(storage_dump_formatter_test, simple) {
    storage_dump_formatter f{};
    auto out = std::cout << f;
    using size_type = kvs::storage_dump::size_type;
    std::int64_t k1{1};
    size_type k1_sz{sizeof(k1)};
    std::int64_t v1{100};
    size_type v1_sz{sizeof(v1)};
    std::int64_t k2{2};
    size_type k2_sz{sizeof(k2)};
    std::int64_t v2{200};
    size_type v2_sz{sizeof(v2)};
    out.write(reinterpret_cast<char*>(&k1_sz), sizeof(k1_sz));
    out.write(reinterpret_cast<char*>(&v1_sz), sizeof(v1_sz));
    out.write(reinterpret_cast<char*>(&k1), sizeof(k1));
    out.write(reinterpret_cast<char*>(&v1), sizeof(v1));

    out.write(reinterpret_cast<char*>(&k2_sz), sizeof(k2_sz));
    out.write(reinterpret_cast<char*>(&v2_sz), sizeof(v2_sz));
    out.write(reinterpret_cast<char*>(&k2), sizeof(k2));
    out.write(reinterpret_cast<char*>(&v2), sizeof(v2));
    out.write(reinterpret_cast<char const*>(&kvs::storage_dump::EOF_MARK), sizeof(kvs::storage_dump::EOF_MARK));
}

TEST_F(storage_dump_formatter_test, dump_db) {
    auto& impl = *db_impl();
    auto& kvs_db = impl.kvs_db();
    auto stg = kvs_db->create_storage("TEST");
    put(
        *kvs_db,
        "TEST",
        mock::create_record<kind::int8>(1),
        mock::create_record<kind::int8>(100)
    );
    put(
        *kvs_db,
        "TEST",
        mock::create_record<kind::int8>(2),
        mock::create_record<kind::int8>(200)
    );

    {
        storage_dump_formatter f{};
        auto out = f.connect(std::cout);
        db_->dump(out, "TEST", 100);
        f.disconnect();
    }
    {
        storage_dump_formatter f{};
        auto out = std::cout << f;
        db_->dump(out, "TEST", 100);
    }
}

}

