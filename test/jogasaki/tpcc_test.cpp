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

#include <gtest/gtest.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/database_impl.h>
#include <jogasaki/api/result_set.h>
namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

class tpcc_test : public ::testing::Test {
public:
};

TEST_F(tpcc_test, empty_graph) {
    jogasaki::api::database db{};
    db.start();

    auto db_impl = api::database::impl::get_impl(db);
    common_cli::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I0", 10, true, 5);
    common_cli::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I1", 10, true, 5);
    common_cli::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I2", 10, true, 5);

    auto rs = db.execute(sql);
    auto it = rs->begin();
    while(it != rs->end()) {
        auto record = it.ref();
        std::stringstream ss{};
        ss << record << *rs->meta();
        LOG(INFO) << ss.str();
        ++it;
    }
    rs->close();
    db.stop();
}

}
