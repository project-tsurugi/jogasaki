/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <atomic>
#include <future>
#include <functional>
#include <cstddef>
#include <memory>
#include <string>

#include <grpcpp/channel.h>
#include <gtest/gtest.h>

#include <takatori/type/primitive.h>

#include <jogasaki/configuration.h>
#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/dist/remote_catalog_manager.h>
#include <jogasaki/dist/remote_server_channel_manager.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/foreign_catalog_statement.h>
#include <jogasaki/model/statement_kind.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/finally.h>

namespace jogasaki::testing {
namespace {

class ddl_catalog_fetcher final : public dist::remote_catalog_fetcher {
public:
    std::atomic_size_t calls{};
    std::function<void()> before_fetch{};
    status result{status::ok};
    status fetch(dist::remote_catalog_reference const&, std::shared_ptr<grpc::Channel> const&,
        std::chrono::system_clock::time_point, dist::remote_catalog_information& out,
        std::string& diagnostic) override {
        ++calls;
        if (before_fetch) before_fetch();
        if (result != status::ok) {
            diagnostic = "catalog fetch failed";
            return result;
        }
        out.schemas.push_back({"public", {0}, {0}});
        out.tables.push_back({41, "t", {0}});
        out.columns.push_back({"id", std::make_shared<takatori::type::int8>(), false});
        out.indices.push_back({42, "pk", 0, {}, {{0}}, {}});
        return status::ok;
    }
};

class foreign_catalog_statement_test : public ::testing::Test {
protected:
    std::shared_ptr<configuration> cfg_ = std::make_shared<configuration>();
    std::shared_ptr<ddl_catalog_fetcher> fetcher_ = std::make_shared<ddl_catalog_fetcher>();
    std::shared_ptr<dist::remote_catalog_manager> catalogs_ = std::make_shared<dist::remote_catalog_manager>(fetcher_);
    dist::remote_server_channel_manager channels_{};
    scheduler::serial_task_scheduler tasks_{};
    scheduler::statement_scheduler scheduler_{cfg_, tasks_};

    void SetUp() override {
        ASSERT_EQ(status::ok, channels_.start({{"worker", "dns:///localhost:12345", false,
            dist::root_certificate_source::none, {}}}));
        ASSERT_EQ(status::ok, catalogs_->start());
    }
    std::unique_ptr<request_context> context() {
        auto result = std::make_unique<request_context>(cfg_);
        result->remote_catalogs_manager(catalogs_);
        result->remote_server_channels(&channels_);
        return result;
    }
};

TEST_F(foreign_catalog_statement_test, scheduler_executes_create_and_drop_and_preserves_old_snapshot) {
    auto create_context = context();
    executor::common::create_foreign_catalog create{{{"remote", "worker"}, false, false}};
    EXPECT_EQ(model::statement_kind::create_foreign_catalog, create.kind());
    scheduler_.schedule(create, *create_context);
    ASSERT_EQ(status::ok, create_context->status_code());
    ASSERT_TRUE(catalogs_->find_catalog("remote"));
    EXPECT_EQ(1U, fetcher_->calls);
    auto old_snapshot = catalogs_->snapshot();
    auto drop_context = context();
    executor::common::drop_foreign_catalog drop{{"remote", false}};
    EXPECT_EQ(model::statement_kind::drop_foreign_catalog, drop.kind());
    scheduler_.schedule(drop, *drop_context);
    EXPECT_EQ(status::ok, drop_context->status_code());
    EXPECT_FALSE(catalogs_->find_catalog("remote"));
    EXPECT_EQ(1U, old_snapshot->catalogs().size());
    EXPECT_TRUE(catalogs_->snapshot()->catalogs().empty());
}

TEST_F(foreign_catalog_statement_test, duplicate_create_reports_status_and_sql_error) {
    executor::common::create_foreign_catalog create{{{"remote", "worker"}, false, false}};
    auto first = context();
    ASSERT_TRUE(create(*first));
    auto duplicate = context();
    scheduler_.schedule(create, *duplicate);
    EXPECT_EQ(status::err_already_exists, duplicate->status_code());
    ASSERT_TRUE(duplicate->error_info());
    EXPECT_EQ(error_code::target_already_exists_exception, duplicate->error_info()->code());
    EXPECT_NE(std::string::npos, duplicate->error_info()->message().find("remote"));
    EXPECT_EQ(1U, fetcher_->calls);
}

TEST_F(foreign_catalog_statement_test, missing_catalog_and_server_report_errors) {
    auto missing_catalog = context();
    executor::common::drop_foreign_catalog drop{{"missing", false}};
    scheduler_.schedule(drop, *missing_catalog);
    EXPECT_EQ(status::err_not_found, missing_catalog->status_code());
    ASSERT_TRUE(missing_catalog->error_info());
    EXPECT_EQ(error_code::target_not_found_exception, missing_catalog->error_info()->code());
    auto missing_server = context();
    executor::common::create_foreign_catalog create{{{"remote", "missing"}, false, false}};
    scheduler_.schedule(create, *missing_server);
    EXPECT_EQ(status::err_not_found, missing_server->status_code());
    ASSERT_TRUE(missing_server->error_info());
    EXPECT_EQ(error_code::target_not_found_exception, missing_server->error_info()->code());
    EXPECT_EQ(0U, fetcher_->calls);
}

TEST_F(foreign_catalog_statement_test, conditional_commands_skip_without_refetch_or_error) {
    executor::common::create_foreign_catalog create{{{"remote", "worker"}, true, true}};
    auto first = context();
    scheduler_.schedule(create, *first);
    ASSERT_EQ(status::ok, first->status_code());
    auto duplicate = context();
    scheduler_.schedule(create, *duplicate);
    EXPECT_EQ(status::ok, duplicate->status_code());
    EXPECT_FALSE(duplicate->error_info());
    EXPECT_EQ(1U, fetcher_->calls);
    auto skip_context = context();
    executor::common::create_foreign_catalog skip{{{"skip", "missing"}, true, true}};
    scheduler_.schedule(skip, *skip_context);
    EXPECT_EQ(status::ok, skip_context->status_code());
    EXPECT_FALSE(catalogs_->find_catalog("skip"));
    auto drop_context = context();
    executor::common::drop_foreign_catalog drop{{"missing", true}};
    scheduler_.schedule(drop, *drop_context);
    EXPECT_EQ(status::ok, drop_context->status_code());
    EXPECT_FALSE(drop_context->error_info());
    EXPECT_EQ(1U, fetcher_->calls);
}

TEST_F(foreign_catalog_statement_test, failed_fetch_reports_diagnostic_and_does_not_publish) {
    fetcher_->result = status::err_io_error;
    auto request = context();
    executor::common::create_foreign_catalog create{{{"remote", "worker"}, false, false}};
    scheduler_.schedule(create, *request);
    EXPECT_EQ(status::err_unknown, request->status_code());
    ASSERT_TRUE(request->error_info());
    EXPECT_EQ(error_code::sql_execution_exception, request->error_info()->code());
    EXPECT_NE(std::string::npos, request->error_info()->message().find("catalog fetch failed"));
    EXPECT_FALSE(catalogs_->find_catalog("remote"));
}

TEST_F(foreign_catalog_statement_test, unavailable_runtime_is_reported_in_context) {
    executor::common::create_foreign_catalog create{{{"remote", "worker"}, false, false}};
    executor::common::drop_foreign_catalog drop{{"remote", false}};
    auto missing_manager = context();
    missing_manager->remote_catalogs_manager({});
    EXPECT_FALSE(create(*missing_manager));
    EXPECT_EQ(status::err_invalid_state, missing_manager->status_code());
    auto missing_drop_manager = context();
    missing_drop_manager->remote_catalogs_manager({});
    EXPECT_FALSE(drop(*missing_drop_manager));
    EXPECT_EQ(status::err_invalid_state, missing_drop_manager->status_code());
    auto missing_channels = context();
    missing_channels->remote_server_channels(nullptr);
    EXPECT_FALSE(create(*missing_channels));
    EXPECT_EQ(status::err_invalid_state, missing_channels->status_code());
    EXPECT_EQ(0U, fetcher_->calls);
}

TEST_F(foreign_catalog_statement_test, statement_owns_command_after_caller_changes_it) {
    dist::create_foreign_catalog_command command{{"remote", "worker"}, false, false};
    executor::common::create_foreign_catalog create{command};
    command.reference.catalog_name = "changed";
    command.reference.remote_server_name = "missing";
    auto request = context();
    scheduler_.schedule(create, *request);
    EXPECT_EQ(status::ok, request->status_code());
    EXPECT_TRUE(catalogs_->find_catalog("remote"));
    EXPECT_FALSE(catalogs_->find_catalog("changed"));
}

TEST_F(foreign_catalog_statement_test, unpublished_name_reservation_reports_cc_exception) {
    std::promise<void> entered{}, resume{};
    auto entered_future = entered.get_future();
    auto proceed = resume.get_future().share();
    fetcher_->before_fetch = [&] { entered.set_value(); proceed.wait(); };
    auto request = context();
    executor::common::create_foreign_catalog statement{{{"remote", "worker"}, true, false}};
    std::future<status> first{};
    std::future<bool> duplicate{};
    bool released{};
    auto release = [&] { if (!released) { resume.set_value(); released = true; } };
    utils::finally release_waiter{release};
    first = std::async(std::launch::async, [&] { return catalogs_->create({"remote", "worker"}, channels_); });
    ASSERT_EQ(std::future_status::ready, entered_future.wait_for(std::chrono::seconds(5)));
    duplicate = std::async(std::launch::async, [&] {
        scheduler_.schedule(statement, *request);
        return !catalogs_->find_catalog("remote");
    });
    auto duplicate_ready = duplicate.wait_for(std::chrono::seconds(5));
    release();
    auto still_unpublished = duplicate.get();
    auto result = first.get();
    fetcher_->before_fetch = {};
    EXPECT_EQ(std::future_status::ready, duplicate_ready);
    EXPECT_TRUE(still_unpublished);
    EXPECT_EQ(status::err_serialization_failure, request->status_code());
    ASSERT_TRUE(request->error_info());
    EXPECT_EQ(error_code::cc_exception, request->error_info()->code());
    EXPECT_EQ(status::ok, result);
    EXPECT_TRUE(catalogs_->find_catalog("remote"));
    EXPECT_EQ(1U, fetcher_->calls.load());
}

} // namespace



} // namespace jogasaki::testing
