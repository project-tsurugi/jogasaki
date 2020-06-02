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
#include <jogasaki/storage/storage_context.h>

#include <string>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/storage/transaction_context.h>

namespace jogasaki::storage {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class storage_context_test : public test_root {};

TEST_F(storage_context_test, construct) {
    storage_context stg{};
}
TEST_F(storage_context_test, open_close) {
    storage_context stg{};
    std::map<std::string, std::string> options{};
    ASSERT_TRUE(stg.open(options));
    ASSERT_TRUE(stg.close());
}

TEST_F(storage_context_test, use_handle) {
    storage_context stg{};
    std::map<std::string, std::string> options{};
    ASSERT_TRUE(stg.open(options));

    auto tx = stg.create_transaction();
    ASSERT_TRUE(tx->control_handle());
    ASSERT_TRUE(tx->handle());
    tx->abort();

    ASSERT_TRUE(stg.close());
}

}

