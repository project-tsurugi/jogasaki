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
#include <jogasaki/executor/process/scanner.h>

#include <string>

#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/storage/transaction_context.h>

#include <jogasaki/basic_record.h>

namespace jogasaki::executor::process {

using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class scanner_test : public test_root {};

TEST_F(scanner_test, simple) {
    auto stg = std::make_shared<storage::storage_context>();
    std::map<std::string, std::string> options{};
    ASSERT_TRUE(stg->open(options));

    record rec{};
    scanner s{{}, stg, test_record_meta1(), rec.ref()};

    s.open();
    s.next();
    ASSERT_EQ(1, rec.key());
    s.next();
    ASSERT_EQ(2, rec.key());
    s.next();
    ASSERT_EQ(3, rec.key());
    s.close();

    stg->close();
}

}

