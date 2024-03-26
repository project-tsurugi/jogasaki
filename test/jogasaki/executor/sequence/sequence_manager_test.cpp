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
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/util/maybe_shared_lock.h>

#include <jogasaki/common_types.h>
#include <jogasaki/executor/sequence/info.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/status.h>

namespace jogasaki::executor::sequence {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace executor;
using namespace takatori::util;

using namespace yugawara;
using namespace yugawara::storage;

class sequence_manager_test :
    public ::testing::Test,
    public kvs_test_base {
public:
    void SetUp() override {
        kvs_db_setup();
    }
    void TearDown() override {
        kvs_db_teardown();
    }
};

TEST_F(sequence_manager_test, simple) {
    configurable_provider provider{};
    provider.add_sequence(storage::sequence{0, "SEQ"});
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequences(nullptr, maybe_shared_ptr{&provider});
    auto* seq = mgr.find_sequence(0);
    ASSERT_TRUE(seq);
    auto cur = seq->get();
    EXPECT_EQ(1, cur.version_);
    EXPECT_EQ(0, cur.value_);
    {
        auto tx = db_->create_transaction();
        EXPECT_EQ(1, seq->next(*tx));
        EXPECT_TRUE(mgr.notify_updates(*tx));
        EXPECT_EQ(status::ok, tx->commit());
    }
    EXPECT_TRUE(mgr.remove_sequence(0));
}

TEST_F(sequence_manager_test, initialize) {
    configurable_provider provider{};
    provider.add_sequence(storage::sequence{1, "SEQ1"});
    manager mgr{ *db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequences(nullptr, maybe_shared_ptr{&provider});

    ASSERT_EQ(1, mgr.sequences().size());
    auto& info0 = *mgr.sequences().at(1).info();
    ASSERT_EQ(1, info0.definition_id());
    ASSERT_EQ(0, info0.initial_value());
    ASSERT_EQ(0, info0.minimum_value());
    ASSERT_EQ(std::numeric_limits<std::int64_t>::max(), info0.maximum_value());
    ASSERT_EQ("SEQ1", info0.name());
    ASSERT_TRUE(info0.cycle());
    ASSERT_EQ(1, info0.increment());
}

TEST_F(sequence_manager_test, sequence_spec) {
    configurable_provider provider{};
    provider.add_sequence(
        storage::sequence{
            111,
            "SEQ1",
            100,
            -2,
            10,
            1000,
            false
        }
    );
    manager mgr{*db_};
    // load mapping from kvs if exists
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequences(nullptr, maybe_shared_ptr{&provider});

    ASSERT_EQ(1, mgr.sequences().size());
    auto& info0 = *mgr.sequences().at(111).info();
    ASSERT_EQ(111, info0.definition_id());
    ASSERT_EQ(100, info0.initial_value());
    ASSERT_EQ(10, info0.minimum_value());
    ASSERT_EQ(1000, info0.maximum_value());
    ASSERT_EQ("SEQ1", info0.name());
    ASSERT_FALSE(info0.cycle());
    ASSERT_EQ(-2, info0.increment());
}

TEST_F(sequence_manager_test, initialize_with_existing_table_entries) {
    configurable_provider provider{};
    provider.add_sequence(storage::sequence{1, "SEQ1"});
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequences(nullptr, maybe_shared_ptr{&provider});
    wait_epochs(10);
    provider.add_sequence(storage::sequence{2, "SEQ2"});
    manager mgr2{*db_};
    EXPECT_EQ(1, mgr2.load_id_map());
    mgr2.register_sequences(nullptr, maybe_shared_ptr{&provider});
    wait_epochs(10);

    ASSERT_EQ(2, mgr2.sequences().size());
    auto& info2_0 = *mgr2.sequences().at(1).info();
    auto& info2_1 = *mgr2.sequences().at(2).info();
    ASSERT_EQ(1, info2_0.definition_id());
    ASSERT_EQ(2, info2_1.definition_id());

    manager mgr3{*db_};
    EXPECT_EQ(2, mgr3.load_id_map());
    mgr3.register_sequences(nullptr, maybe_shared_ptr{&provider});
    wait_epochs(10);

    ASSERT_EQ(2, mgr3.sequences().size());
    auto& info3_0 = *mgr3.sequences().at(1).info();
    auto& info3_1 = *mgr3.sequences().at(2).info();
    ASSERT_EQ(info3_0, info2_0);
    ASSERT_EQ(info3_1, info2_1);
}

TEST_F(sequence_manager_test, sequence_manipulation) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequence(nullptr, 1, "SEQ1");
    mgr.register_sequence(nullptr, 2, "SEQ2");
    mgr.register_sequence(nullptr, 3, "SEQ3");
    auto* s = mgr.find_sequence(2);
    ASSERT_TRUE(s);
    auto& body = mgr.sequences().at(2);
    EXPECT_EQ(*body.info(), s->info());

    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(0, vv.value_);
        v1 = vv.version_;
    }

    auto tx = db_->create_transaction();
    auto val = s->next(*tx);
    EXPECT_EQ(1, val);
    {
        auto vv = s->get();
        EXPECT_LT(v1, vv.version_);
        EXPECT_EQ(1, vv.value_);
        v1 = vv.version_;
    }

    val = s->next(*tx);
    EXPECT_EQ(2, val);
    {
        auto vv = s->get();
        EXPECT_LT(v1, vv.version_);
        EXPECT_EQ(2, vv.value_);
        v1 = vv.version_;
    }

}



TEST_F(sequence_manager_test, sequence_manipulation_varieties) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        100,
        -2,
        10,
        1000,
        false
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(100, vv.value_);
        v1 = vv.version_;
    }

    auto tx = db_->create_transaction();
    auto val = s->next(*tx);
    EXPECT_EQ(98, val);
    {
        auto vv = s->get();
        EXPECT_LT(v1, vv.version_);
        EXPECT_EQ(98, vv.value_);
        v1 = vv.version_;
    }

    val = s->next(*tx);
    EXPECT_EQ(96, val);
    {
        auto vv = s->get();
        EXPECT_LT(v1, vv.version_);
        EXPECT_EQ(96, vv.value_);
        v1 = vv.version_;
    }
}

TEST_F(sequence_manager_test, cycle_positive_incr) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        6,
        3,
        2,
        9,
        true
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(6, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(9, s->next(*tx));
    EXPECT_EQ(2, s->next(*tx));
    EXPECT_EQ(5, s->next(*tx));
    EXPECT_EQ(8, s->next(*tx));
    EXPECT_EQ(2, s->next(*tx));
}

TEST_F(sequence_manager_test, cycle_negative_incr) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        5,
        -3,
        2,
        9,
        true
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(5, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(2, s->next(*tx));
    EXPECT_EQ(9, s->next(*tx));
    EXPECT_EQ(6, s->next(*tx));
    EXPECT_EQ(3, s->next(*tx));
    EXPECT_EQ(9, s->next(*tx));
}

TEST_F(sequence_manager_test, no_cycle_positive_incr) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        4,
        3,
        2,
        9,
        false
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(4, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(7, s->next(*tx));
    EXPECT_EQ(9, s->next(*tx));
    EXPECT_EQ(9, s->next(*tx));
}

TEST_F(sequence_manager_test, no_cycle_negative_incr) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        6,
        -3,
        2,
        9,
        false
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(6, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(3, s->next(*tx));
    EXPECT_EQ(2, s->next(*tx));
    EXPECT_EQ(2, s->next(*tx));
}

TEST_F(sequence_manager_test, cycle_positive_incr_around_intmax) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    constexpr static sequence_value mx = std::numeric_limits<sequence_value>::max();
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        mx-2,
        3,
        mx-3,
        mx,
        true
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(mx-2, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(mx-3, s->next(*tx));
    EXPECT_EQ(mx, s->next(*tx));
    EXPECT_EQ(mx-3, s->next(*tx));
}

TEST_F(sequence_manager_test, no_cycle_positive_incr_around_intmax) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    constexpr static sequence_value mx = std::numeric_limits<sequence_value>::max();
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        mx-2,
        3,
        mx-3,
        mx,
        false
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(mx-2, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(mx, s->next(*tx));
    EXPECT_EQ(mx, s->next(*tx));
}

TEST_F(sequence_manager_test, cycle_negative_incr_around_intmin) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    constexpr static sequence_value mi = std::numeric_limits<sequence_value>::min();
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        mi+2,
        -3,
        mi,
        mi+3,
        true
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(mi+2, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(mi+3, s->next(*tx));
    EXPECT_EQ(mi, s->next(*tx));
    EXPECT_EQ(mi+3, s->next(*tx));
}

TEST_F(sequence_manager_test, no_cycle_negative_incr_around_intmin) {
    manager mgr{*db_};
    EXPECT_EQ(0, mgr.load_id_map());
    constexpr static sequence_value mi = std::numeric_limits<sequence_value>::min();
    mgr.register_sequence(
        nullptr,
        111,
        "SEQ1",
        mi+2,
        -3,
        mi,
        mi+3,
        false
    );
    auto* s = mgr.find_sequence(111);
    ASSERT_TRUE(s);
    sequence_version v1{};
    {
        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(mi+2, vv.value_);
        v1 = vv.version_;
    }
    auto tx = db_->create_transaction();
    EXPECT_EQ(mi, s->next(*tx));
    EXPECT_EQ(mi, s->next(*tx));
}

TEST_F(sequence_manager_test, drop_sequence) {
    {
        manager mgr{*db_};
        EXPECT_EQ(0, mgr.load_id_map());
        mgr.register_sequence(nullptr, 1, "SEQ1");
        mgr.register_sequence(nullptr, 2, "SEQ3");
        auto* s = mgr.find_sequence(2);
        ASSERT_TRUE(s);

        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(0, vv.value_);
        auto tx = db_->create_transaction();
        auto val = s->next(*tx);
        EXPECT_EQ(1, val);
        mgr.notify_updates(*tx);
        ASSERT_EQ(status::ok, tx->commit());
        mgr.remove_sequence(2);
        wait_epochs(10);
    }
    {
        manager mgr{*db_};
        EXPECT_EQ(1, mgr.load_id_map());
        mgr.register_sequence(nullptr, 2, "SEQ3", 100);
        auto* s = mgr.find_sequence(2);
        ASSERT_TRUE(s);

        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        auto tx = db_->create_transaction();
        auto val = s->next(*tx);
        EXPECT_EQ(101, val);
        mgr.notify_updates(*tx);
        ASSERT_EQ(status::ok, tx->commit());

        mgr.remove_sequence(2);
    }
}

TEST_F(sequence_manager_test, save_and_recover) {
    if (jogasaki::kvs::implementation_id() != "memory") {
        GTEST_SKIP() << "shirakami wp build doesn't support recovery yet";
    }
    {
        manager mgr{*db_};
        EXPECT_EQ(0, mgr.load_id_map());
        mgr.register_sequence(nullptr, 1, "SEQ1");
        mgr.register_sequence(nullptr, 2, "SEQ3");
        auto* s = mgr.find_sequence(2);
        ASSERT_TRUE(s);

        auto vv = s->get();
        EXPECT_EQ(1, vv.version_);
        EXPECT_EQ(0, vv.value_);
        auto tx = db_->create_transaction();
        auto val = s->next(*tx);
        EXPECT_EQ(1, val);
        mgr.notify_updates(*tx);
        ASSERT_EQ(status::ok, tx->commit());
        wait_epochs(10);
    }
    // expecting the transaction became durable and sequence is updated
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    {
        manager mgr{*db_};
        EXPECT_EQ(2, mgr.load_id_map());
        mgr.register_sequence(nullptr, 2, "SEQ3");
        auto* s = mgr.find_sequence(2);
        ASSERT_TRUE(s);

        auto vv = s->get();
        EXPECT_EQ(2, vv.version_);
        EXPECT_EQ(1, vv.value_);
        auto tx = db_->create_transaction();
        auto val = s->next(*tx);
        EXPECT_EQ(2, val);
        mgr.notify_updates(*tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
}

}

