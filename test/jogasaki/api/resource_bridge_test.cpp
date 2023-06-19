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
#include <jogasaki/api/resource/bridge.h>

#include <regex>
#include <sstream>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::api::resource {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class resource_bridge_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

static constexpr std::string_view default_configuration {  // NOLINT
    "[sql]\n"
        "thread_pool_size=5\n"
        "lazy_worker=false\n"
        "enable_index_join=false\n"
        "stealing_enabled=true\n"
        "default_partitions=5\n"
        "stealing_wait=1\n"
        "task_polling_wait=0\n"
        "tasked_write=true\n"
        "lightweight_job_level=0\n"
        "enable_hybrid_scheduler=true\n"
    "[datastore]\n"
        "log_location=\n"
        "logging_max_parallelism=112\n"
};

TEST_F(resource_bridge_test, resource_cfg) {
    std::stringstream ss{
        "[sql]\n"
        "thread_pool_size=99\n"
        "lazy_worker=true\n"
        "enable_index_join=true\n"
        "[datastore]\n"
        "log_location=LOCATION\n"
        "logging_max_parallelism=111\n"
    };
    tateyama::api::configuration::whole cfg{ss, default_configuration};

    auto c = api::resource::convert_config(cfg);
    EXPECT_EQ(99, c->thread_pool_size());
    EXPECT_TRUE(c->lazy_worker());
    EXPECT_EQ("LOCATION", c->db_location());
    EXPECT_EQ(111, c->max_logging_parallelism());
    EXPECT_TRUE(c->enable_index_join());
}

TEST_F(resource_bridge_test, cfg_default_value) {
    // tateyama cfg has default value, which precedes default of jogasaki::configuration
    std::stringstream ss{
        "[datastore]\n"
    };
    tateyama::api::configuration::whole cfg{ss, default_configuration};
    auto c = api::resource::convert_config(cfg);
    jogasaki::configuration jc{};
    EXPECT_NE(jc.max_logging_parallelism(), c->max_logging_parallelism());
}

}
