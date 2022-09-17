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

#include <sstream>
#include <future>
#include <thread>
#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/latch.h>
#include <jogasaki/test_utils/temporary_folder.h>

#include <tateyama/api/server/mock/request_response.h>
#include <jogasaki/utils/msgpack_utils.h>

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
namespace sql = jogasaki::proto::sql;
using ValueCase = sql::request::Parameter::ValueCase;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;

// verify msgpack behavior
class msgpack_test :
    public ::testing::Test {

public:

    void SetUp() override {
        temporary_.prepare();
    }

    void TearDown() override {
        temporary_.clean();
    }

    test::temporary_folder temporary_{};
};

TEST_F(msgpack_test, pack) {
    // verify msgpack::pack behavior
    std::stringstream ss;
    {
        msgpack::pack(ss, msgpack::type::nil_t()); // nil can be put without specifying the type
        std::int32_t i32{1};
        msgpack::pack(ss, i32);
        i32 = 100000;
        msgpack::pack(ss, i32);
        std::int64_t i64{2};
        msgpack::pack(ss, i64);
        float f4{10.0};
        msgpack::pack(ss, f4);
        float f8{11.0};
        msgpack::pack(ss, f8);
        msgpack::pack(ss, "ABC"sv);
    }

    std::string str{ss.str()};
    std::size_t offset{};
    std::int32_t i32{};
    std::int64_t i64{};
    EXPECT_FALSE(extract(str, i32, offset));  // nil can be read as any type
    ASSERT_EQ(1, offset);
    EXPECT_TRUE(extract(str, i32, offset));
    EXPECT_EQ(1, i32);
    ASSERT_EQ(2, offset);
    EXPECT_TRUE(extract(str, i32, offset));
    EXPECT_EQ(100000, i32);
    ASSERT_EQ(7, offset);
    EXPECT_TRUE(extract(str, i64, offset));
    EXPECT_EQ(2, i64);
    ASSERT_EQ(8, offset);
}

}
