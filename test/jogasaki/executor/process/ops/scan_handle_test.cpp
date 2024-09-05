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
#include <memory>
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/test_root.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;
using namespace jogasaki::kvs;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class scan_handle_test :
    public test_root,
    public kvs_test_base {
public:
    void SetUp() override {
        kvs_db_setup();
    }
    void TearDown() override {
        kvs_db_teardown();
    }
};

using k = meta::field_type_kind;

TEST_F(scan_handle_test, commit_without_releasing_scan_handle) {
    // special scenario forgetting release iterator before commit
    // This caused reading wrong result.
    auto t1 = db_->create_storage("T1");
    {
        {
            std::string s{"a"};
            auto tx = db_->create_transaction();
            ASSERT_EQ(status::ok, t1->content_put(*tx, {s.data(), s.size()}, ""));
            ASSERT_EQ(status::ok, tx->commit());
        }
        {
            auto tx = wrap(db_->create_transaction());
            std::unique_ptr<iterator> it{};
            ASSERT_EQ(status::ok, t1->content_scan(*tx, "", end_point_kind::unbound, "", end_point_kind::unbound, it));
            ASSERT_EQ(status::ok, it->next());

            std::string_view key{};
            std::string_view value{};
            ASSERT_EQ(status::ok, it->read_key(key));
            ASSERT_EQ("a", key);
            ASSERT_EQ(status::ok, it->read_value(value));
            ASSERT_EQ("", value);
//            it.reset();  // forget releasing iterator here
            ASSERT_EQ(status::ok, tx->commit());
        }
        {
            std::string s{"a"};
            auto tx = db_->create_transaction();
            ASSERT_EQ(status::ok, t1->content_put(*tx, {s.data(), s.size()}, "A"));
            ASSERT_EQ(status::ok, tx->commit());
        }
        {
            auto tx = wrap(db_->create_transaction());
            std::unique_ptr<iterator> it{};
            ASSERT_EQ(status::ok, t1->content_scan(*tx, "", end_point_kind::unbound, "", end_point_kind::unbound, it));
            ASSERT_EQ(status::ok, it->next());

            std::string_view key{};
            std::string_view value{};
            ASSERT_EQ(status::ok, it->read_key(key));
            ASSERT_EQ("a", key);
            ASSERT_EQ(status::ok, it->read_value(value));
            ASSERT_EQ("A", value);
            it.reset();
            ASSERT_EQ(status::ok, tx->commit());
        }
    }
}

}

