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

#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/api/field_type_kind.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

/**
 * @brief test database api
 */
class database_test : public ::testing::Test {

};

TEST_F(database_test, simple) {
    std::string sql = "select * from T0";
    auto db = api::create_database();
    db->start();
    db->register_variable("p0", api::field_type_kind::int8);
    db->register_variable("p1", api::field_type_kind::float8);
    std::unique_ptr<api::prepared_statement> prepared{};
    ASSERT_EQ(status::ok, db->prepare("INSERT INTO T0 (C0, C1) VALUES(:p0, :p1)", prepared));
    {
        auto tx = db->create_transaction();
        for(std::size_t i=0; i < 2; ++i) {
            auto ps = api::create_parameter_set();
            ps->set_int8("p0", i);
            ps->set_float8("p1", 10.0*i);
            std::unique_ptr<api::executable_statement> exec{};
            ASSERT_EQ(status::ok,db->resolve(*prepared, *ps, exec));
            ASSERT_EQ(status::ok,tx->execute(*exec));
        }
        tx->commit();
    }

    {
        auto tx = db->create_transaction();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db->create_executable("select * from T0 order by C0", exec));
//        db->explain(*exec, std::cout);
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok,tx->execute(*exec, rs));
        auto it = rs->iterator();
        std::size_t count = 0;
        while(it->has_next()) {
            std::stringstream ss{};
            auto* record = it->next();
            ss << *record;
            LOG(INFO) << ss.str();
            ++count;
        }
        EXPECT_EQ(2, count);
        tx->commit();
    }
    {
        // reuse prepared statement
        std::unique_ptr<api::prepared_statement> prep{};
        ASSERT_EQ(status::ok,db->prepare("select * from T0 where C0 = :p0", prep));
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 0);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db->resolve(*prep, *ps, exec));
//        db->explain(*exec, std::cout);
        auto f = [&]() {
            auto tx = db->create_transaction();
            std::unique_ptr<api::result_set> rs{};
            ASSERT_EQ(status::ok,tx->execute(*exec, rs));
            auto it = rs->iterator();
            std::size_t count = 0;
            while(it->has_next()) {
                std::stringstream ss{};
                auto* record = it->next();
                ss << *record;
                LOG(INFO) << ss.str();
                ++count;
            }
            EXPECT_EQ(1, count);
            tx->commit();
        };
        f();
        ps->set_int8("p0", 1);
        ASSERT_EQ(status::ok,db->resolve(*prep, *ps, exec));
        prep.reset();
        ps.reset();
        f();
    }
    db->stop();
}

TEST_F(database_test, update_with_host_variable) {
    auto db = api::create_database();
    db->start();
    db->register_variable("p1", api::field_type_kind::float8);
    std::unique_ptr<api::prepared_statement> prepared{};
    ASSERT_EQ(status::ok, db->prepare("UPDATE T0 SET C1 = :p1 WHERE C0 = 0", prepared));
    std::unique_ptr<api::executable_statement> insert{};
    ASSERT_EQ(status::ok, db->create_executable("INSERT INTO T0 (C0, C1) VALUES(0, 10.0)", insert));
    {
        auto tx = db->create_transaction();
        ASSERT_EQ(status::ok,tx->execute(*insert));
        tx->commit();
    }
    {
        auto tx = db->create_transaction();
        auto ps = api::create_parameter_set();
        ps->set_float8("p1", 0.0);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db->resolve(*prepared, *ps, exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    db->stop();
}

}
