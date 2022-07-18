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
#include <jogasaki/logship/log_event_listener.h>

#include <regex>
#include <gtest/gtest.h>
#include <future>

#include <takatori/type/int.h>
#include <takatori/util/downcast.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "../api/api_test_base.h"

namespace jogasaki::logship {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace takatori;
using namespace yugawara::storage;
using nullity = yugawara::variable::nullity;

using takatori::util::unsafe_downcast;

class logship_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_logship(true);
        cfg->max_logging_parallelism(1);
        db_setup(cfg);
        auto* impl = db_impl();
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(logship_test, simple) {
    auto t = std::make_shared<table>(
        100,
        "LOGSHIP",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "C1", type::int8(), nullity{false} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t->definition_id(),
        t,
        t->simple_name(),
        std::initializer_list<yugawara::storage::index::key>{
            t->columns()[0],
        },
        std::initializer_list<yugawara::storage::index::column_ref>{
            t->columns()[1],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));
    execute_statement("INSERT INTO LOGSHIP (C0, C1) VALUES (1, 1)");
}

TEST_F(logship_test, no_callback) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
}

TEST_F(logship_test, types) {
    auto t = std::make_shared<table>(
        100,
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "K1", type::character(type::varying), nullity{false} },
            column{ "K2", type::int8(), nullity{false} },
            column{ "K3", type::float8 (), nullity{false} },
            column{ "K4", type::character(~type::varying), nullity{false} },
            column{ "K5", type::int4(), nullity{true} },
            column{ "K6", type::float4(), nullity{true} },
            column{ "V1", type::character(type::varying), nullity{false} },
            column{ "V2", type::int8(), nullity{false} },
            column{ "V3", type::float8 (), nullity{false} },
            column{ "V4", type::character(~type::varying), nullity{false} },
            column{ "V5", type::int4(), nullity{true} },
            column{ "V6", type::float4(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t->definition_id(),
        t,
        t->simple_name(),
        std::initializer_list<yugawara::storage::index::key>{
            t->columns()[0],
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
            t->columns()[4],
            t->columns()[5],
            t->columns()[6],
        },
        std::initializer_list<yugawara::storage::index::column_ref>{
            t->columns()[7],
            t->columns()[8],
            t->columns()[9],
            t->columns()[10],
            t->columns()[11],
            t->columns()[12],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));

    execute_statement("INSERT INTO TEST (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (1, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (2, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0)");
}
}
