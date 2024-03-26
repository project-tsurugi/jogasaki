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
#include <gtest/gtest.h>

#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/server.h>

#include <jogasaki/api/kvsservice/resource.h>
#include <jogasaki/api/kvsservice/service.h>
#include <jogasaki/api/kvsservice/status.h>
#include <jogasaki/api/kvsservice/store.h>
#include <jogasaki/api/kvsservice/transaction.h>
#include <jogasaki/api/kvsservice/transaction_option.h>
#include <jogasaki/api/kvsservice/transaction_type.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/service/bridge.h>

#include "test_utils.h"

namespace jogasaki::api::kvsservice {

class server_test: public ::testing::Test {
    public:
        void SetUp() override {
        }

        void TearDown() override {
        }
    };

TEST_F(server_test, DISABLED_resouce_check) {
    tateyama::framework::server sv {tateyama::framework::boot_mode::database_server,
                                    default_configuration()};
    tateyama::framework::add_core_components(sv);
    sv.add_resource(std::make_shared<jogasaki::api::resource::bridge>());
    sv.add_service(std::make_shared<jogasaki::api::service::bridge>());
    auto rsc = std::make_shared<jogasaki::api::kvsservice::resource>();
    sv.add_resource(rsc);
    sv.add_service(std::make_shared<jogasaki::api::kvsservice::service>());
    EXPECT_TRUE(sv.setup());
    EXPECT_TRUE(sv.start());
    auto store = rsc->store();
    EXPECT_NE(store, nullptr);
    EXPECT_TRUE(sv.shutdown());
}

TEST_F(server_test, DISABLED_store_check) {
    tateyama::framework::server sv {tateyama::framework::boot_mode::database_server,
                                    default_configuration()};
    tateyama::framework::add_core_components(sv);
    sv.add_resource(std::make_shared<jogasaki::api::resource::bridge>());
    sv.add_service(std::make_shared<jogasaki::api::service::bridge>());
    auto rsc = std::make_shared<jogasaki::api::kvsservice::resource>();
    sv.add_resource(rsc);
    sv.add_service(std::make_shared<jogasaki::api::kvsservice::service>());
    EXPECT_TRUE(sv.setup());
    EXPECT_TRUE(sv.start());
    auto store = rsc->store();
    EXPECT_NE(store, nullptr);
    table_areas wp{};
    transaction_option opt {transaction_type::occ, wp};
    std::shared_ptr<transaction> tx{};
    {
        auto s = store->begin_transaction(opt, tx);
        EXPECT_EQ(s, status::ok);
        EXPECT_NE(tx, nullptr);
        EXPECT_NE(tx.get(), nullptr);
        EXPECT_NE(tx->system_id(), 0);
    }
    {
        auto tx2 = store->find_transaction(tx->system_id());
        EXPECT_NE(tx2, nullptr);
        EXPECT_NE(tx2.get(), nullptr);
        EXPECT_EQ(tx2->system_id(), tx->system_id());
    }
    {
        auto id = tx->system_id();
        auto s = tx->commit();
        EXPECT_EQ(s, status::ok);
        auto s2 = store->dispose_transaction(id);
        EXPECT_EQ(s2, status::ok);
        auto tx2 = store->find_transaction(id);
        EXPECT_EQ(tx2, nullptr);
    }
    EXPECT_TRUE(sv.shutdown());
}

}
