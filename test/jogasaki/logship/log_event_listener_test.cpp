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

using takatori::util::unsafe_downcast;

using namespace takatori;
using namespace yugawara::storage;
using nullity = yugawara::variable::nullity;

class log_event_listener_test :
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

TEST_F(log_event_listener_test, simple) {
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

    auto* impl = db_impl();
    log_event_listener listener{impl->tables()};

    auto cfg = std::make_shared<configuration>();
    cfg->max_logging_parallelism(1);
    ASSERT_TRUE(listener.init(*cfg));

    std::vector<sharksfin::LogRecord> recs{};
    recs.emplace_back(
        sharksfin::LogRecord{sharksfin::LogOperation::INSERT, "\x08\x00\x00\x00\x00\x00\x00\x00"sv, "\x7F\xff\xff\xff\xff\xff\xff\xfe"sv, 0UL, 0UL, 100UL}
    );

    ASSERT_TRUE(listener(0, &*recs.begin(), &*recs.end()));

    ASSERT_TRUE(listener.deinit());
}

}
