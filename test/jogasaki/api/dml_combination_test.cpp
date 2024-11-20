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
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class dml_combination_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        auto& impl = jogasaki::api::impl::get_impl(*db_);
        jogasaki::utils::add_benchmark_tables(*impl.tables());
        jogasaki::utils::add_test_tables(*impl.tables());
        jogasaki::executor::register_kvs_storage(*impl.kvs_db(), *impl.tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(dml_combination_test, delete_insert_delete) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");

    auto tx = utils::create_transaction(*db_);
    execute_statement("DELETE FROM T0 WHERE C0=2", *tx);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 20.0)", *tx);
    execute_statement("DELETE FROM T0 WHERE C0=2", *tx);
    {
        // verify by range scan
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", *tx, result);
        ASSERT_EQ(2, result.size());
    }
    {
        // verify by point query
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0=2", *tx, result);
        ASSERT_EQ(0, result.size());
    }
    ASSERT_EQ(status::ok, tx->commit());
}

}
