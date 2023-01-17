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
#include <jogasaki/api.h>

#include <thread>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include <takatori/type/int.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include "api_test_base.h"
#include <jogasaki/kvs/id.h>
#include <jogasaki/utils/storage_dump_formatter.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/test_utils/secondary_index.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using namespace yugawara;
using namespace yugawara::storage;

namespace type = takatori::type;
using nullity = yugawara::variable::nullity;

using kind = jogasaki::meta::field_type_kind;
/**
 * @brief test database recovery
 */
class recovery2_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->single_thread(true);
        cfg->prepare_benchmark_tables(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

// regression test - create new table after recovery on prepare_benchmark_tables = true failed
TEST_F(recovery2_test, create_sequence_after_recovery) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        execute_statement("CREATE TABLE T (C0 INT)");
    }
}

}
