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
#include <jogasaki/executor/process/impl/ops/scan.h>

#include <string>

#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/storage/transaction_context.h>

#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::process::impl::ops {

using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class scan_test : public test_root {};

TEST_F(scan_test, simple) {
    auto stg = std::make_shared<storage::storage_context>();
    std::map<std::string, std::string> options{};

    mock::record rec{};

    relation::scan* node{};
    scan s{{}, 0, {}, test_record_meta1()};
//    scan_context ctx(stg, block_scope_info{});

//    s(ctx);
}

}

