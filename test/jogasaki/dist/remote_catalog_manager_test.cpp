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
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/type/primitive.h>
#include <takatori/type/table.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/aggregate/provider.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/provider.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/provider.h>

#include <jogasaki/dist/remote_catalog_command.h>
#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/dist/remote_catalog_manager.h>
#include <jogasaki/dist/remote_function_id_registry.h>

namespace jogasaki::testing {
namespace {
using namespace dist;
class test_catalog_fetcher final : public remote_catalog_fetcher {
public:
    std::atomic_size_t calls{};
    status result{status::ok};
    remote_catalog_information info{};
    std::function<void()> before_fetch{};
    status fetch(remote_catalog_reference const&, std::shared_ptr<grpc::Channel> const&,
        std::chrono::system_clock::time_point deadline, remote_catalog_information& out, std::string&) override {
        ++calls;
        EXPECT_GT(deadline, std::chrono::system_clock::now());
        if (before_fetch) before_fetch();
        out = info;
        return result;
    }
};
using type_ptr = remote_catalog_information::type_ptr;
type_ptr int8_type() { return std::make_shared<takatori::type::int8>(); }

std::shared_ptr<yugawara::function::declaration const> first_function(yugawara::function::provider const& provider) {
    std::shared_ptr<yugawara::function::declaration const> result{};
    provider.each([&](std::shared_ptr<yugawara::function::declaration const> const& d) { if (!result) result = d; });
    return result;
}

std::shared_ptr<yugawara::aggregate::declaration const> first_aggregate(yugawara::aggregate::provider const& provider) {
    std::shared_ptr<yugawara::aggregate::declaration const> result{};
    provider.each([&](std::shared_ptr<yugawara::aggregate::declaration const> const& d) { if (!result) result = d; });
    return result;
}

class remote_catalog_manager_test : public ::testing::Test {
protected:
    std::shared_ptr<test_catalog_fetcher> fetcher = std::make_shared<test_catalog_fetcher>();
    // owned outside the manager, as the database does
    std::shared_ptr<remote_function_id_registry> ids = std::make_shared<remote_function_id_registry>();
    remote_catalog_manager manager{fetcher, remote_catalog_manager::default_fetch_timeout, ids};
    remote_server_channel_manager channels{};
    std::string diagnostic{};
    void SetUp() override {
        ASSERT_EQ(status::ok, channels.start({{"worker", "dns:///localhost:12345", false,
            root_certificate_source::none, {}}}));
        fetcher->info.schemas.push_back({"public", {0}, {0}});
        fetcher->info.tables.push_back({41, "t", {0}});
        fetcher->info.columns.push_back({"id", std::make_shared<takatori::type::int8>(), false});
        fetcher->info.indices.push_back({42, "pk", 0, {}, {{0}}, {}});
        ASSERT_EQ(status::ok, manager.start());
    }
};
TEST_F(remote_catalog_manager_test, start_is_empty_without_fetch) {
    ASSERT_TRUE(manager.entries());
    EXPECT_TRUE(manager.entries()->empty());
    EXPECT_EQ(0, fetcher->calls);
}
TEST_F(remote_catalog_manager_test, create_publishes_only_complete_catalog_and_drop_keeps_old_snapshot) {
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels, diagnostic));
    auto old = manager.snapshot();
    auto catalog = manager.find_catalog("remote");
    ASSERT_TRUE(catalog);
    EXPECT_EQ(1, old->catalogs().size());
    EXPECT_EQ(status::ok, manager.drop("remote"));
    EXPECT_FALSE(manager.find_catalog("remote"));
    EXPECT_TRUE(manager.snapshot()->catalogs().empty());
    EXPECT_EQ(catalog, old->catalogs().front());
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels));
    EXPECT_NE(catalog.get(), manager.find_catalog("remote").get());
}
TEST_F(remote_catalog_manager_test, remote_definition_ids_are_kept_separate_from_transfer_indices) {
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels));
    auto snapshot = manager.snapshot();
    auto schema = manager.find_catalog("remote")->schema_provider().find("public");
    ASSERT_TRUE(schema);
    auto table = schema->storage_provider().find_table("t");
    auto index = schema->storage_provider().find_index("pk");
    ASSERT_TRUE(table); ASSERT_TRUE(index);
    auto table_id = snapshot->remote_definition(*table);
    auto index_id = snapshot->remote_definition(*index);
    ASSERT_TRUE(table_id); ASSERT_TRUE(index_id);
    EXPECT_EQ(41, table_id->remote_definition_id);
    EXPECT_EQ(42, index_id->remote_definition_id);
    EXPECT_EQ("worker", table_id->remote_server_name);
}
TEST_F(remote_catalog_manager_test, stopped_instance_cannot_restart_but_new_instance_starts_empty) {
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels));
    ASSERT_EQ(status::ok, manager.stop());
    EXPECT_EQ(status::err_invalid_state, manager.start());
    remote_catalog_manager restarted{fetcher};
    EXPECT_EQ(status::ok, restarted.start());
    ASSERT_TRUE(restarted.entries());
    EXPECT_TRUE(restarted.entries()->empty());
    EXPECT_EQ(1, fetcher->calls);
}

TEST_F(remote_catalog_manager_test, failed_fetch_preserves_published_view) {
    ASSERT_EQ(status::ok, manager.create({"first", "worker"}, channels));
    auto before = manager.snapshot();
    fetcher->result = status::err_io_error;
    EXPECT_NE(status::ok, manager.create({"second", "worker"}, channels, diagnostic));
    EXPECT_EQ(before, manager.snapshot());
    EXPECT_FALSE(manager.find_catalog("second"));
    EXPECT_FALSE(diagnostic.empty());
}
TEST_F(remote_catalog_manager_test, malformed_references_do_not_publish) {
    fetcher->info.tables.front().columns = {99};
    EXPECT_NE(status::ok, manager.create({"bad", "worker"}, channels, diagnostic));
    EXPECT_TRUE(manager.entries()->empty());
    EXPECT_FALSE(diagnostic.empty());
}
std::shared_ptr<takatori::type::table const> table_type(std::vector<std::string> const& names) {
    std::vector<takatori::type::table::column_type> columns{};
    for (auto const& name : names) {
        columns.emplace_back(name, int8_type());
    }
    return std::make_shared<takatori::type::table>(std::move(columns));
}

void add_functions(remote_catalog_information& info) {
    info.functions.push_back({7, "f", {int8_type()}, int8_type(),
        {yugawara::function::function_feature::scalar_function}});
    info.aggregate_functions.push_back({7, "agg", {int8_type()}, int8_type(), true});
    info.schemas.front().functions = {0};
    info.schemas.front().aggregate_functions = {0};
}

TEST_F(remote_catalog_manager_test, functions_are_restored_with_coordinator_ids_and_remote_identity) {
    add_functions(fetcher->info);
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels, diagnostic)) << diagnostic;
    auto snapshot = manager.snapshot();
    auto schema = manager.find_catalog("remote")->schema_provider().find("public");
    ASSERT_TRUE(schema);
    auto function = first_function(schema->function_provider());
    auto aggregate = first_aggregate(schema->set_function_provider());
    ASSERT_TRUE(function);
    ASSERT_TRUE(aggregate);
    EXPECT_EQ("f", function->name());
    EXPECT_TRUE(function->features().contains(yugawara::function::function_feature::scalar_function));
    EXPECT_FALSE(function->features().contains(yugawara::function::function_feature::table_valued_function));
    EXPECT_TRUE(aggregate->incremental());
    // coordinator IDs come from the registry, not from the remote ID nor the transfer index
    EXPECT_GE(function->definition_id(), remote_function_id_registry::default_minimum_id);
    EXPECT_GE(aggregate->definition_id(), remote_function_id_registry::default_minimum_id);
    EXPECT_TRUE(ids->function_reserved(function->definition_id()));
    EXPECT_TRUE(ids->aggregate_function_reserved(aggregate->definition_id()));
    auto function_identity = snapshot->remote_definition(*function);
    auto aggregate_identity = snapshot->remote_definition(*aggregate);
    ASSERT_TRUE(function_identity);
    ASSERT_TRUE(aggregate_identity);
    EXPECT_EQ(remote_declaration_kind::function, function_identity->kind);
    EXPECT_EQ(remote_declaration_kind::aggregate_function, aggregate_identity->kind);
    EXPECT_EQ(7, function_identity->remote_definition_id);
    EXPECT_EQ(7, aggregate_identity->remote_definition_id);
    EXPECT_EQ("worker", function_identity->remote_server_name);
    // functions have no remote executable location: only tables are placed
    EXPECT_EQ(1, snapshot->locations().size());
}

TEST_F(remote_catalog_manager_test, function_ids_stay_reserved_until_last_reference_is_released) {
    add_functions(fetcher->info);
    ASSERT_EQ(status::ok, manager.create({"first", "worker"}, channels));
    auto old = manager.snapshot();
    auto function = first_function(manager.find_catalog("first")->schema_provider().find("public")->function_provider());
    ASSERT_TRUE(function);
    auto id = function->definition_id();
    ASSERT_EQ(status::ok, manager.drop("first"));
    // still referenced: a new catalog must not take the ID
    ASSERT_EQ(status::ok, manager.create({"second", "worker"}, channels));
    auto other = first_function(manager.find_catalog("second")->schema_provider().find("public")->function_provider());
    ASSERT_TRUE(other);
    EXPECT_NE(id, other->definition_id());
    EXPECT_TRUE(ids->function_reserved(id));
    old.reset();
    EXPECT_TRUE(ids->function_reserved(id));
    function.reset();
    EXPECT_FALSE(ids->function_reserved(id));
    // released IDs may be reused by a later create
    ASSERT_EQ(status::ok, manager.create({"third", "worker"}, channels));
    auto reused = first_function(manager.find_catalog("third")->schema_provider().find("public")->function_provider());
    ASSERT_TRUE(reused);
    EXPECT_EQ(id, reused->definition_id());
}

TEST_F(remote_catalog_manager_test, unpublished_create_releases_function_ids) {
    add_functions(fetcher->info);
    fetcher->before_fetch = [&] { EXPECT_EQ(status::ok, manager.stop()); };
    EXPECT_EQ(status::err_invalid_state, manager.create({"remote", "worker"}, channels));
    EXPECT_EQ(0, ids->reserved_count());
}

TEST_F(remote_catalog_manager_test, invalid_function_declarations_reject_whole_catalog) {
    add_functions(fetcher->info);
    auto const original = fetcher->info;
    type_ptr table = table_type({"c"});
    std::vector<remote_catalog_information> invalid{};
    auto edit = [&](auto&& change) { auto info = original; change(info); invalid.push_back(std::move(info)); };
    edit([](auto& i) { i.functions[0].features = {}; });
    edit([](auto& i) { i.functions[0].features = {yugawara::function::function_feature::scalar_function,
        yugawara::function::function_feature::table_valued_function}; });
    edit([&](auto& i) { i.functions[0].return_type = table; });
    edit([](auto& i) { i.functions[0].features = {yugawara::function::function_feature::table_valued_function}; });
    edit([&](auto& i) { i.functions[0].features = {yugawara::function::function_feature::table_valued_function};
        i.functions[0].return_type = table_type({"c", "c"}); });
    edit([&](auto& i) { i.functions[0].parameter_types = {table}; });
    edit([](auto& i) { i.functions[0].parameter_types = {nullptr}; });
    edit([](auto& i) { i.aggregate_functions[0].return_type = nullptr; });
    edit([](auto& i) { i.functions[0].name.clear(); });
    edit([](auto& i) { i.schemas[0].functions = {1}; });
    edit([](auto& i) { i.schemas[0].aggregate_functions.clear(); });
    edit([](auto& i) { i.functions.push_back(i.functions[0]); i.schemas[0].functions.push_back(1); });
    for (auto const& info : invalid) {
        fetcher->info = info;
        EXPECT_NE(status::ok, manager.create({"bad", "worker"}, channels, diagnostic));
        EXPECT_TRUE(manager.entries()->empty());
        EXPECT_FALSE(diagnostic.empty());
        EXPECT_EQ(0, ids->reserved_count());
    }
}

TEST_F(remote_catalog_manager_test, table_valued_function_keeps_table_return_type) {
    add_functions(fetcher->info);
    auto& function = fetcher->info.functions[0];
    function.features = {yugawara::function::function_feature::table_valued_function};
    function.return_type = table_type({"a", "b"});
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels, diagnostic)) << diagnostic;
    auto restored = first_function(manager.find_catalog("remote")->schema_provider().find("public")->function_provider());
    ASSERT_TRUE(restored);
    EXPECT_TRUE(restored->features().contains(yugawara::function::function_feature::table_valued_function));
    EXPECT_TRUE(*function.return_type == restored->return_type());
}

TEST_F(remote_catalog_manager_test, functions_require_registry_but_tables_do_not) {
    remote_catalog_manager without_registry{fetcher};
    ASSERT_EQ(status::ok, without_registry.start());
    ASSERT_EQ(status::ok, without_registry.create({"tables", "worker"}, channels));
    add_functions(fetcher->info);
    EXPECT_EQ(status::err_invalid_state, without_registry.create({"functions", "worker"}, channels, diagnostic));
    EXPECT_FALSE(without_registry.find_catalog("functions"));
}

TEST_F(remote_catalog_manager_test, described_catalog_functions_round_trip_with_new_ids) {
    auto functions = std::make_shared<yugawara::function::configurable_provider>();
    std::vector<type_ptr> parameters{int8_type()};
    functions->add(yugawara::function::declaration{77, "f", int8_type(), parameters,
        {yugawara::function::function_feature::scalar_function}});
    auto aggregates = std::make_shared<yugawara::aggregate::configurable_provider>();
    aggregates->add(yugawara::aggregate::declaration{78, "agg", int8_type(), parameters, false});
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public",
        std::make_shared<yugawara::storage::configurable_provider>(), {}, functions, aggregates});
    yugawara::schema::catalog source{"source", std::nullopt, schemas};
    remote_catalog_information described{};
    ASSERT_EQ(status::ok, describe_remote_catalog(source, described, diagnostic)) << diagnostic;
    ASSERT_EQ(1, described.functions.size());
    ASSERT_EQ(1, described.aggregate_functions.size());
    EXPECT_EQ(77, described.functions[0].remote_definition_id);
    EXPECT_EQ(78, described.aggregate_functions[0].remote_definition_id);
    EXPECT_FALSE(described.aggregate_functions[0].incremental);
    fetcher->info = described;
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels, diagnostic)) << diagnostic;
    auto restored = first_function(manager.find_catalog("remote")->schema_provider().find("public")->function_provider());
    ASSERT_TRUE(restored);
    EXPECT_NE(77, restored->definition_id());
    EXPECT_EQ(77, manager.snapshot()->remote_definition(*restored)->remote_definition_id);
    ASSERT_EQ(1, restored->parameter_types().size());
    EXPECT_EQ(takatori::type::int8::tag, restored->return_type().kind());
}

TEST(remote_function_id_registry_test, id_spaces_are_separate_and_skip_local_ids) {
    auto ids = std::make_shared<remote_function_id_registry>(100);
    ids->exclude_function(100);
    ids->exclude_function(102);
    std::shared_ptr<remote_function_id_lease> lease{};
    ASSERT_EQ(status::ok, ids->reserve(2, 1, lease));
    EXPECT_EQ((std::vector<remote_function_id_lease::definition_id_type>{101, 103}), lease->functions());
    EXPECT_EQ((std::vector<remote_function_id_lease::definition_id_type>{100}), lease->aggregate_functions());
    std::shared_ptr<remote_function_id_lease> second{};
    ASSERT_EQ(status::ok, ids->reserve(1, 0, second));
    EXPECT_EQ(104, second->functions().front());
    lease.reset();
    EXPECT_FALSE(ids->function_reserved(101));
    EXPECT_TRUE(ids->function_reserved(104));
}

TEST(remote_function_id_registry_test, never_uses_zero_and_requires_shared_owner) {
    auto ids = std::make_shared<remote_function_id_registry>(0);
    std::shared_ptr<remote_function_id_lease> lease{};
    ASSERT_EQ(status::ok, ids->reserve(1, 1, lease));
    EXPECT_NE(0, lease->functions().front());
    EXPECT_NE(0, lease->aggregate_functions().front());
    remote_function_id_registry unowned{};
    std::shared_ptr<remote_function_id_lease> orphan{};
    EXPECT_EQ(status::err_invalid_state, unowned.reserve(1, 0, orphan));
    EXPECT_EQ(0, unowned.reserved_count());
}

TEST(remote_function_id_registry_test, lease_outliving_registry_is_harmless) {
    auto ids = std::make_shared<remote_function_id_registry>();
    std::shared_ptr<remote_function_id_lease> lease{};
    ASSERT_EQ(status::ok, ids->reserve(1, 0, lease));
    ids.reset();
    lease.reset();
}
TEST_F(remote_catalog_manager_test, missing_server_and_duplicate_name_do_not_fetch) {
    EXPECT_NE(status::ok, manager.create({"bad", "missing"}, channels));
    EXPECT_EQ(0, fetcher->calls);
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels));
    EXPECT_NE(status::ok, manager.create({"remote", "worker"}, channels));
    EXPECT_EQ(1, fetcher->calls);
}
TEST_F(remote_catalog_manager_test, stop_during_fetch_never_publishes) {
    std::promise<void> entered{}, resume{};
    auto wait = resume.get_future().share();
    fetcher->before_fetch = [&] { entered.set_value(); wait.wait(); };
    auto create = std::async(std::launch::async, [&] { return manager.create({"remote", "worker"}, channels); });
    auto entry = entered.get_future().wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, entry);
    EXPECT_EQ(status::ok, manager.stop());
    resume.set_value();
    EXPECT_EQ(status::err_invalid_state, create.get());
    EXPECT_FALSE(manager.entries());
    EXPECT_FALSE(manager.snapshot());
}
TEST_F(remote_catalog_manager_test, conditional_commands_skip_only_existing_objects) {
    create_foreign_catalog_command create{{"remote", "worker"}, true, false};
    EXPECT_EQ(status::ok, execute_create_foreign_catalog(create, manager, channels, diagnostic));
    EXPECT_EQ(status::ok, execute_create_foreign_catalog(create, manager, channels, diagnostic));
    EXPECT_EQ(1, fetcher->calls);
    EXPECT_EQ(status::ok, execute_drop_foreign_catalog({"remote", true}, manager, diagnostic));
    EXPECT_EQ(status::ok, execute_drop_foreign_catalog({"remote", true}, manager, diagnostic));
    EXPECT_EQ(status::err_not_found, execute_drop_foreign_catalog({"remote", false}, manager, diagnostic));
    EXPECT_EQ(status::ok, execute_create_foreign_catalog({{"skip", "missing"}, false, true}, manager, channels, diagnostic));
    EXPECT_FALSE(manager.find_catalog("skip"));
}
TEST_F(remote_catalog_manager_test, conditional_create_does_not_report_success_for_unpublished_reservation) {
    std::promise<void> entered{}, resume{};
    auto wait = resume.get_future().share();
    fetcher->before_fetch = [&] { entered.set_value(); wait.wait(); };
    fetcher->result = status::err_io_error;
    auto create = std::async(std::launch::async, [&] { return manager.create({"remote", "worker"}, channels); });
    EXPECT_EQ(std::future_status::ready, entered.get_future().wait_for(std::chrono::seconds(5)));
    EXPECT_EQ(status::err_serialization_failure,
        execute_create_foreign_catalog({{"remote", "worker"}, true, false}, manager, channels, diagnostic));
    resume.set_value();
    EXPECT_NE(status::ok, create.get());
    EXPECT_FALSE(manager.find_catalog("remote"));
}

TEST_F(remote_catalog_manager_test, same_name_concurrent_create_does_not_fetch_twice) {
    std::promise<void> entered{}, resume{};
    auto wait = resume.get_future().share();
    fetcher->before_fetch = [&] { entered.set_value(); wait.wait(); };
    auto create = std::async(std::launch::async, [&] { return manager.create({"remote", "worker"}, channels); });
    EXPECT_EQ(std::future_status::ready, entered.get_future().wait_for(std::chrono::seconds(5)));
    EXPECT_NE(status::ok, manager.create({"remote", "worker"}, channels));
    resume.set_value();
    EXPECT_EQ(status::ok, create.get());
    EXPECT_EQ(1, fetcher->calls);
}
} // namespace
TEST_F(remote_catalog_manager_test, reserved_local_server_is_rejected_before_fetch_and_publication) {
    auto before = manager.snapshot();
    EXPECT_EQ(status::err_invalid_argument, manager.create({"bad", "local"}, channels, diagnostic));
    EXPECT_NE(std::string::npos, diagnostic.find("reserved"));
    EXPECT_EQ(0, fetcher->calls);
    EXPECT_EQ(before, manager.snapshot());
    create_foreign_catalog_command command{{"bad", "local"}, true, false};
    EXPECT_EQ(status::err_invalid_argument,
        execute_create_foreign_catalog(command, manager, channels, diagnostic));
    EXPECT_EQ(0, fetcher->calls);
    EXPECT_EQ(before, manager.snapshot());
    EXPECT_EQ(status::ok, manager.create({"valid", "worker"}, channels, diagnostic));
}

} // namespace jogasaki::testing
