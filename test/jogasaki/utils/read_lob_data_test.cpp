/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/utils/read_lob_file.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/test_utils/create_file.h>

#include <gtest/gtest.h>

namespace jogasaki::utils {

class read_lob_data_test :
    public ::testing::Test, public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        datastore::get_datastore(true);  // reset cache for datastore object as db setup recreates it
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(read_lob_data_test, basic) {
    auto path1 = path()+"/file1.dat";
    create_file(path1, "ABC");

    std::string out{};
    std::shared_ptr<error::error_info> error{};
    ASSERT_EQ(status::ok, read_lob_file(path1, out, error));
    EXPECT_EQ("ABC", out);
}

TEST_F(read_lob_data_test, large_file_4M) {
    auto path1 = path()+"/file1.dat";
    std::string content(4*1024*1024, 'A');
    create_file(path1, content);

    std::string out{};
    std::shared_ptr<error::error_info> error{};
    ASSERT_EQ(status::ok, read_lob_file(path1, out, error));
    EXPECT_EQ(content, out);
}

}  // namespace jogasaki::utils
