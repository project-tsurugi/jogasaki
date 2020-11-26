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
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/executor/process/impl/ops/emit.h>

namespace jogasaki::executor::process::impl::ops {

using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class emit_test : public test_root {};

TEST_F(emit_test, simple) {
    auto stg = std::make_shared<kvs::database>();
    std::map<std::string, std::string> options{};
    ASSERT_TRUE(stg->open(options));

    memory::page_pool pool;
    memory::monotonic_paged_memory_resource record_resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    auto store = std::make_shared<data::record_store>(&record_resource, &varlen_resource, test_record_meta1());

    relation::emit const& node{};
    emit e{0, {}, 0, node.columns()};

    test::record rec0{0, 0.0};
    test::record rec1{1, 1.0};
    test::record rec2{2, 2.0};
//    e(accessor::record_ref{&rec0, sizeof(record)});
//    e.write(accessor::record_ref{&rec1, sizeof(record)});
//    e.write(accessor::record_ref{&rec2, sizeof(record)});
//    ASSERT_EQ(3, store->count());
}

}

