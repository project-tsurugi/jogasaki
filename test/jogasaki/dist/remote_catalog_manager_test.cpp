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
#include <algorithm>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <new>
#include <stdexcept>
#include <limits>
#include <set>
#include <thread>
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

#include <jogasaki/utils/finally.h>
#include <jogasaki/dist/remote_catalog_command.h>
#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/dist/remote_catalog_manager.h>
#include <jogasaki/dist/remote_function_id_registry.h>
#include <jogasaki/dist/snapshot_remote_catalog_fetcher.h>

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


TEST_F(remote_catalog_manager_test, snapshot_fetcher_describes_published_catalog_and_rejects_wrong_reference) {
    add_functions(fetcher->info);
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels, diagnostic));
    // This source contains coordinator declarations: their definition IDs are not the original remote IDs.
    snapshot_remote_catalog_fetcher source{manager.snapshot()};
    auto references = source.references();
    ASSERT_EQ(1, references.size());
    EXPECT_EQ("remote", references[0].catalog_name);
    EXPECT_EQ("worker", references[0].remote_server_name);
    remote_catalog_information restored{};
    ASSERT_EQ(status::ok, source.fetch(references[0], {}, {}, restored, diagnostic)) << diagnostic;
    ASSERT_EQ(1, restored.tables.size());
    EXPECT_EQ("t", restored.tables[0].name);
    ASSERT_EQ(1, restored.columns.size());
    EXPECT_EQ("id", restored.columns[0].name);
    EXPECT_FALSE(restored.columns[0].nullable);
    EXPECT_EQ(takatori::type::int8::tag, restored.columns[0].type->kind());
    ASSERT_EQ(1, restored.functions.size());
    EXPECT_EQ("f", restored.functions[0].name);
    auto before = restored.tables[0].name;
    auto counts = [](remote_catalog_information const& info) {
        return std::vector<std::size_t>{info.schemas.size(), info.tables.size(), info.columns.size(),
            info.indices.size(), info.functions.size(), info.aggregate_functions.size()};
    };
    auto before_counts = counts(restored);
    EXPECT_EQ(status::err_not_found, source.fetch({"missing", "worker"}, {}, {}, restored, diagnostic));
    EXPECT_EQ(before, restored.tables[0].name);
    EXPECT_EQ(before_counts, counts(restored));
    EXPECT_EQ(status::err_not_found, source.fetch({"remote", "other"}, {}, {}, restored, diagnostic));
    EXPECT_EQ(before, restored.tables[0].name);
    EXPECT_EQ(before_counts, counts(restored));
}

TEST_F(remote_catalog_manager_test, snapshot_fetcher_rejects_absent_or_inconsistent_placement) {
    ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels, diagnostic));
    auto snapshot = manager.snapshot();
    auto locations = snapshot->locations();
    ASSERT_FALSE(locations.empty());
    locations.clear();
    auto unplaced = std::make_shared<remote_catalog_snapshot>(snapshot->catalogs(), locations);
    snapshot_remote_catalog_fetcher missing{unplaced};
    remote_catalog_information result{};
    EXPECT_TRUE(missing.references().empty());
    EXPECT_EQ(status::err_not_found, missing.fetch({"remote", "worker"}, {}, {}, result, diagnostic));
    snapshot_remote_catalog_fetcher absent{{}};
    EXPECT_TRUE(absent.references().empty());
    EXPECT_EQ(status::err_not_found, absent.fetch({"remote", "worker"}, {}, {}, result, diagnostic));

    fetcher->info.tables.push_back({43, "t2", {1}});
    fetcher->info.columns.push_back({"id2", int8_type(), true});
    fetcher->info.schemas[0].tables.push_back(1);
    ASSERT_EQ(status::ok, manager.create({"mixed", "worker"}, channels, diagnostic));
    auto mixed = manager.snapshot();
    auto placements = mixed->locations();
    auto table = manager.find_catalog("mixed")->schema_provider().find("public")->storage_provider().find_table("t2");
    ASSERT_TRUE(table);
    placements.at(table.get()) = "other";
    snapshot_remote_catalog_fetcher inconsistent{
        std::make_shared<remote_catalog_snapshot>(mixed->catalogs(), placements)};
    EXPECT_EQ(status::err_not_found, inconsistent.fetch({"mixed", "worker"}, {}, {}, result, diagnostic));
    auto refs = inconsistent.references();
    ASSERT_EQ(1, refs.size());
    EXPECT_EQ("remote", refs[0].catalog_name);
}

TEST_F(remote_catalog_manager_test, snapshot_fetcher_does_not_select_a_catalog_without_tables) {
    fetcher->info = {};
    fetcher->info.schemas.push_back({"public", {}, {}});
    add_functions(fetcher->info);
    // Development fetcher uses table placement; RPC can transfer a function-only catalog.
    ASSERT_EQ(status::ok, manager.create({"functions", "worker"}, channels, diagnostic)) << diagnostic;
    snapshot_remote_catalog_fetcher source{manager.snapshot()};
    EXPECT_TRUE(source.references().empty());
    remote_catalog_information result{};
    EXPECT_EQ(status::err_not_found, source.fetch({"functions", "worker"}, {}, {}, result, diagnostic));
}

TEST(remote_function_id_registry_test, exhaustion_is_atomic_and_preserves_output_lease) {
    using id_type = remote_function_id_lease::definition_id_type;
    auto maximum = std::numeric_limits<id_type>::max();
    auto ids = std::make_shared<remote_function_id_registry>(maximum);
    std::shared_ptr<remote_function_id_lease> held{};
    ASSERT_EQ(status::ok, ids->reserve(1, 0, held));
    EXPECT_EQ(maximum, held->functions().front());
    auto output = held;
    EXPECT_EQ(status::err_resource_limit_reached, ids->reserve(0, 2, output));
    EXPECT_EQ(held, output);
    EXPECT_EQ(1, ids->reserved_count());
    EXPECT_FALSE(ids->aggregate_function_reserved(maximum));
    held.reset(); output.reset();
    // First space can allocate, second cannot: neither space may be committed.
    EXPECT_EQ(status::err_resource_limit_reached, ids->reserve(1, 2, output));
    EXPECT_FALSE(output);
    EXPECT_EQ(0, ids->reserved_count());
    ASSERT_EQ(status::ok, ids->reserve(1, 1, output));
    EXPECT_EQ(maximum, output->functions().front());
    EXPECT_EQ(maximum, output->aggregate_functions().front());
    output.reset();
    EXPECT_EQ(0, ids->reserved_count());
}

TEST(remote_function_id_registry_test, concurrent_reservations_are_unique_and_released_ids_are_reused) {
    auto ids = std::make_shared<remote_function_id_registry>(100);
    constexpr std::size_t workers = 8;
    std::vector<std::shared_ptr<remote_function_id_lease>> leases(workers);
    std::vector<status> results(workers);
    std::vector<std::thread> threads{};
    std::promise<void> start{};
    auto ready = start.get_future().share();
    for (std::size_t i = 0; i < workers; ++i) {
        threads.emplace_back([&, i] { ready.wait(); results[i] = ids->reserve(2, 1, leases[i]); });
    }
    start.set_value();
    for (auto& thread : threads) thread.join();
    std::set<remote_function_id_lease::definition_id_type> regular{}, aggregate{};
    for (std::size_t i = 0; i < workers; ++i) {
        ASSERT_EQ(status::ok, results[i]);
        for (auto id : leases[i]->functions()) EXPECT_TRUE(regular.insert(id).second);
        for (auto id : leases[i]->aggregate_functions()) EXPECT_TRUE(aggregate.insert(id).second);
    }
    EXPECT_EQ(workers * 3, ids->reserved_count());
    threads.clear();
    // Release one lease while another request reserves. Kept leases must remain reserved.
    std::shared_ptr<remote_function_id_lease> replacement{};
    status next{};
    std::thread release{[&] { leases[0].reset(); }};
    std::thread reserve{[&] { next = ids->reserve(2, 1, replacement); }};
    release.join(); reserve.join();
    ASSERT_EQ(status::ok, next);
    for (std::size_t i = 1; i < workers; ++i) {
        for (auto id : leases[i]->functions()) EXPECT_TRUE(ids->function_reserved(id));
        for (auto id : leases[i]->aggregate_functions()) EXPECT_TRUE(ids->aggregate_function_reserved(id));
        for (auto id : replacement->functions()) {
            EXPECT_EQ(0, std::count(leases[i]->functions().begin(), leases[i]->functions().end(), id));
        }
        for (auto id : replacement->aggregate_functions()) {
            EXPECT_EQ(0, std::count(leases[i]->aggregate_functions().begin(), leases[i]->aggregate_functions().end(), id));
        }
    }
    leases.clear(); replacement.reset();
    EXPECT_EQ(0, ids->reserved_count());
    ASSERT_EQ(status::ok, ids->reserve(1, 1, replacement));
    EXPECT_EQ(100, replacement->functions().front());
    EXPECT_EQ(100, replacement->aggregate_functions().front());
}



TEST_F(remote_catalog_manager_test, malformed_table_index_and_schema_metadata_preserves_published_snapshot) {
    ASSERT_EQ(status::ok, manager.create({"existing", "worker"}, channels));
    auto existing = manager.find_catalog("existing");
    add_functions(fetcher->info);
    auto const valid = fetcher->info;
    struct invalid_case {
        char const* name;
        char const* diagnostic;
        std::function<void(remote_catalog_information&)> edit;
    };
    auto second_table = [](remote_catalog_information& info) {
        info.tables.push_back({51, "other", {1}});
        info.columns.push_back({"other_column", int8_type(), false});
        info.schemas[0].tables.push_back(1);
    };
    std::vector<invalid_case> cases{
        {"foreign column", "index column is not in its table", [=](auto& i) {
            second_table(i); i.indices[0].keys[0].column = 1;
        }},
        {"foreign schema", "index table is in another schema", [=](auto& i) {
            second_table(i); i.schemas[0].tables.pop_back();
            i.schemas.push_back({"other_schema", {1}, {}}); i.indices[0].table = 1;
            i.indices[0].keys[0].column = 1;
        }},
        {"duplicate table name", "duplicate table name", [=](auto& i) {
            second_table(i); i.tables[1].name = i.tables[0].name;
        }},
        {"duplicate index name", "duplicate index name", [](auto& i) {
            i.indices.push_back(i.indices[0]); i.schemas[0].indices.push_back(1);
        }},
        {"no columns", "table has no columns", [](auto& i) {
            i.tables[0].columns.clear(); i.columns.clear(); i.indices.clear(); i.schemas[0].indices.clear();
        }},
        {"unsupported column type", "unsupported column type", [](auto& i) {
            i.columns[0].type = std::make_shared<takatori::type::table>(
                std::vector<takatori::type::table::column_type>{});
        }},
        {"multiple table owners", "table referenced by multiple owners", [](auto& i) {
            i.schemas.push_back({"other_schema", {0}, {}});
        }},
        {"duplicate owner reference", "table referenced twice by one owner", [](auto& i) {
            i.schemas[0].tables.push_back(0);
        }},
        {"orphan table", "orphan table", [](auto& i) { i.schemas[0].tables.clear(); }},
        {"orphan index", "orphan index", [](auto& i) { i.schemas[0].indices.clear(); }},
        {"orphan column", "orphan column", [](auto& i) {
            i.columns.push_back({"unowned", int8_type(), false});
        }},
        {"multiple column owners", "column referenced by multiple owners", [](auto& i) {
            i.tables.push_back({51, "other", {0}}); i.schemas[0].tables.push_back(1);
        }},
        {"duplicate column owner reference", "column referenced twice by one owner", [](auto& i) {
            i.tables[0].columns.push_back(0);
        }},
        {"empty name", "invalid table name", [](auto& i) { i.tables[0].name.clear(); }},
        {"invalid UTF-8", "invalid table name", [](auto& i) { i.tables[0].name = std::string(1, static_cast<char>(0xFF)); }},
        {"embedded NUL", "invalid table name", [](auto& i) { i.tables[0].name = std::string{"bad\0name", 8}; }},
        {"duplicate schema name", "duplicate schema name", [](auto& i) {
            i.schemas.push_back({"public", {}, {}});
        }},
    };
    for (auto const& entry : cases) {
        SCOPED_TRACE(entry.name);
        auto before = manager.snapshot();
        fetcher->info = valid;
        entry.edit(fetcher->info);
        EXPECT_EQ(status::err_unknown, manager.create({"invalid", "worker"}, channels, diagnostic));
        EXPECT_NE(std::string::npos, diagnostic.find(entry.diagnostic)) << diagnostic;
        EXPECT_EQ(before, manager.snapshot());
        EXPECT_EQ(existing, manager.find_catalog("existing"));
        EXPECT_FALSE(manager.find_catalog("invalid"));
        EXPECT_EQ(0U, ids->reserved_count());
        // The failed name reservation must be reusable.
        fetcher->info = valid;
        ASSERT_EQ(status::ok, manager.create({"invalid", "worker"}, channels, diagnostic)) << diagnostic;
        ASSERT_EQ(status::ok, manager.drop("invalid"));
    }
}

TEST_F(remote_catalog_manager_test, different_name_concurrent_creates_keep_both_catalogs) {
    std::promise<void> entered{}, resume{};
    auto entered_future = entered.get_future();
    auto proceed = resume.get_future().share();
    std::atomic_size_t arrivals{};
    fetcher->before_fetch = [&] {
        if (arrivals.fetch_add(1) == 0) { entered.set_value(); proceed.wait(); }
    };
    std::future<status> first{}, second{};
    bool released{};
    auto release = [&] { if (!released) { resume.set_value(); released = true; } };
    // Destroyed before either future, including exception and assertion exits.
    utils::finally release_waiter{release};
    first = std::async(std::launch::async, [&] { return manager.create({"first", "worker"}, channels); });
    ASSERT_EQ(std::future_status::ready, entered_future.wait_for(std::chrono::seconds(5)));
    second = std::async(std::launch::async, [&] { return manager.create({"second", "worker"}, channels); });
    auto second_ready = second.wait_for(std::chrono::seconds(5));
    release();
    auto second_result = second.get();
    auto first_result = first.get();
    fetcher->before_fetch = {};
    EXPECT_EQ(std::future_status::ready, second_ready);
    ASSERT_EQ(status::ok, second_result);
    ASSERT_EQ(status::ok, first_result);
    EXPECT_EQ(2U, manager.entries()->size());
    EXPECT_EQ(2U, manager.snapshot()->catalogs().size());
    EXPECT_TRUE(manager.find_catalog("first"));
    EXPECT_TRUE(manager.find_catalog("second"));
    EXPECT_EQ(2U, fetcher->calls);
}

TEST_F(remote_catalog_manager_test, dropped_snapshot_preserves_placement_and_definition_identity) {
    ASSERT_EQ(status::ok, manager.create({"branch", "worker"}, channels));
    auto old = manager.snapshot();
    auto catalog = manager.find_catalog("branch");
    ASSERT_TRUE(catalog);
    auto schema = catalog->schema_provider().find("public");
    ASSERT_TRUE(schema);
    auto table = schema->storage_provider().find_table("t");
    auto index = schema->storage_provider().find_index("pk");
    ASSERT_TRUE(table); ASSERT_TRUE(index);
    ASSERT_EQ(status::ok, manager.drop("branch"));
    EXPECT_TRUE(manager.snapshot()->catalogs().empty());
    ASSERT_EQ(status::ok, manager.create({"branch", "worker"}, channels));
    EXPECT_NE(catalog, manager.find_catalog("branch"));
    EXPECT_EQ(nullptr, manager.snapshot()->remote_server(*table));
    EXPECT_EQ(nullptr, manager.snapshot()->remote_definition(*table));
    ASSERT_NE(nullptr, old->remote_server(*table));
    EXPECT_EQ("worker", *old->remote_server(*table));
    auto table_id = old->remote_definition(*table);
    auto index_id = old->remote_definition(*index);
    ASSERT_NE(nullptr, table_id); ASSERT_NE(nullptr, index_id);
    EXPECT_EQ("worker", table_id->remote_server_name);
    EXPECT_EQ(remote_declaration_kind::table, table_id->kind);
    EXPECT_EQ(41U, table_id->remote_definition_id);
    EXPECT_EQ(remote_declaration_kind::index, index_id->kind);
    EXPECT_EQ(42U, index_id->remote_definition_id);
}


TEST_F(remote_catalog_manager_test, zero_storage_ids_and_all_index_feature_combinations_are_preserved) {
    using feature = yugawara::storage::index_feature;
    std::vector<feature> const values{feature::primary, feature::find, feature::scan,
        feature::unique, feature::unique_constraint};
    fetcher->info.tables[0].remote_definition_id = 0;
    fetcher->info.indices[0].remote_definition_id = 0;
    for (std::size_t mask = 0; mask < (std::size_t{1} << values.size()); ++mask) {
        SCOPED_TRACE(mask);
        yugawara::storage::index_feature_set features{};
        for (std::size_t bit = 0; bit < values.size(); ++bit) {
            if ((mask & (std::size_t{1} << bit)) != 0) features.insert(values[bit]);
        }
        fetcher->info.indices[0].features = features;
        ASSERT_EQ(status::ok, manager.create({"remote", "worker"}, channels, diagnostic)) << diagnostic;
        auto snapshot = manager.snapshot();
        auto schema = manager.find_catalog("remote")->schema_provider().find("public");
        auto table = schema->storage_provider().find_table("t");
        auto index = schema->storage_provider().find_index("pk");
        ASSERT_TRUE(table); ASSERT_TRUE(index);
        EXPECT_EQ(features, index->features());
        ASSERT_TRUE(snapshot->remote_definition(*table));
        ASSERT_TRUE(snapshot->remote_definition(*index));
        EXPECT_EQ(0U, snapshot->remote_definition(*table)->remote_definition_id);
        EXPECT_EQ(0U, snapshot->remote_definition(*index)->remote_definition_id);
        ASSERT_EQ(status::ok, manager.drop("remote"));
    }
}

TEST_F(remote_catalog_manager_test, create_command_condition_matrix_checks_server_before_catalog) {
    for (bool existing : {false, true}) {
        for (bool server : {false, true}) {
            for (bool if_catalog_not_exists : {false, true}) {
                for (bool if_server_exists : {false, true}) {
                    SCOPED_TRACE(::testing::Message{} << existing << server
                        << if_catalog_not_exists << if_server_exists);
                    remote_catalog_manager catalogs{fetcher};
                    ASSERT_EQ(status::ok, catalogs.start());
                    if (existing) ASSERT_EQ(status::ok, catalogs.create({"remote", "worker"}, channels));
                    auto before = catalogs.snapshot();
                    auto calls = fetcher->calls.load();
                    auto expected = !server ? (if_server_exists ? status::ok : status::err_not_found)
                        : existing ? (if_catalog_not_exists ? status::ok : status::err_already_exists)
                        : status::ok;
                    EXPECT_EQ(expected, execute_create_foreign_catalog(
                        {{"remote", server ? "worker" : "missing"}, if_catalog_not_exists, if_server_exists},
                        catalogs, channels, diagnostic));
                    bool created = server && !existing;
                    EXPECT_EQ(calls + (created ? 1U : 0U), fetcher->calls.load());
                    EXPECT_EQ(existing || created, static_cast<bool>(catalogs.find_catalog("remote")));
                    if (!created) EXPECT_EQ(before, catalogs.snapshot());
                    EXPECT_EQ(expected == status::ok, diagnostic.empty());
                }
            }
        }
    }
}

TEST_F(remote_catalog_manager_test, reference_names_are_checked_before_conditional_skip) {
    std::vector<std::string> invalid{"", "__reserved", std::string{"bad\0name", 8},
        std::string(1, static_cast<char>(0xFF))};
    auto before = manager.snapshot();
    for (auto const& name : invalid) {
        EXPECT_FALSE(valid_remote_catalog_identifier(name));
        for (auto const& reference : {remote_catalog_reference{name, "worker"},
                                     remote_catalog_reference{"remote", name}}) {
            EXPECT_EQ(status::err_invalid_argument, manager.create(reference, channels, diagnostic));
            EXPECT_EQ(status::err_invalid_argument,
                execute_create_foreign_catalog({reference, true, true}, manager, channels, diagnostic));
            EXPECT_EQ(before, manager.snapshot());
            EXPECT_FALSE(diagnostic.empty());
        }
    }
    EXPECT_EQ(0U, fetcher->calls.load());
    EXPECT_EQ(status::err_invalid_argument,
        execute_create_foreign_catalog({{"remote", "local"}, true, true}, manager, channels, diagnostic));
    EXPECT_EQ(before, manager.snapshot());
    EXPECT_EQ(0U, fetcher->calls.load());
    // Reference identifiers preserve quoted-identifier spelling instead of imposing an ASCII token grammar.
    for (auto const& name : {std::string{"remote catalog"}, std::string{"販売"}}) {
        EXPECT_TRUE(valid_remote_catalog_identifier(name));
        ASSERT_EQ(status::ok, manager.create({name, "worker"}, channels, diagnostic)) << diagnostic;
        EXPECT_TRUE(manager.find_catalog(name));
        EXPECT_EQ(status::ok, execute_drop_foreign_catalog({name, false}, manager, diagnostic));
    }
}


TEST_F(remote_catalog_manager_test, unavailable_manager_does_not_turn_conditional_commands_into_success) {
    ASSERT_EQ(status::ok, manager.stop());
    EXPECT_EQ(status::err_invalid_state,
        execute_create_foreign_catalog({{"remote", "missing"}, true, true}, manager, channels, diagnostic));
    EXPECT_FALSE(diagnostic.empty());
    EXPECT_EQ(status::err_invalid_state, execute_drop_foreign_catalog({"missing", true}, manager, diagnostic));
    EXPECT_FALSE(diagnostic.empty());
    EXPECT_EQ(0U, fetcher->calls.load());
}


TEST_F(remote_catalog_manager_test, unresolved_function_ids_reject_whole_catalog_and_keep_id_reservations) {
    add_functions(fetcher->info);
    // is_defined() excludes only the unresolved marker, not system/builtin ID partitions.
    fetcher->info.functions[0].remote_definition_id = 1;
    fetcher->info.aggregate_functions[0].remote_definition_id = 1;
    auto valid = fetcher->info;
    ASSERT_EQ(status::ok, manager.create({"existing", "worker"}, channels, diagnostic)) << diagnostic;
    ASSERT_EQ(2U, ids->reserved_count());
    for (bool aggregate : {false, true}) {
        SCOPED_TRACE(aggregate);
        auto before = manager.snapshot();
        fetcher->info = valid;
        if (aggregate) {
            auto invalid = valid.aggregate_functions[0];
            invalid.name = "unresolved";
            invalid.remote_definition_id = yugawara::aggregate::declaration::unresolved_definition_id;
            fetcher->info.aggregate_functions.push_back(invalid);
            fetcher->info.schemas[0].aggregate_functions.push_back(valid.aggregate_functions.size());
        } else {
            auto invalid = valid.functions[0];
            invalid.name = "unresolved";
            invalid.remote_definition_id = yugawara::function::declaration::unresolved_definition_id;
            fetcher->info.functions.push_back(invalid);
            fetcher->info.schemas[0].functions.push_back(valid.functions.size());
        }
        EXPECT_EQ(status::err_unknown, manager.create({"candidate", "worker"}, channels, diagnostic));
        EXPECT_NE(std::string::npos, diagnostic.find(aggregate
            ? "unresolved remote definition ID of aggregate function: unresolved"
            : "unresolved remote definition ID of function: unresolved"));
        EXPECT_EQ(before, manager.snapshot());
        EXPECT_FALSE(manager.find_catalog("candidate"));
        EXPECT_EQ(2U, ids->reserved_count());
        fetcher->info = valid;
        ASSERT_EQ(status::ok, manager.create({"candidate", "worker"}, channels, diagnostic)) << diagnostic;
        EXPECT_EQ(4U, ids->reserved_count());
        ASSERT_EQ(status::ok, manager.drop("candidate"));
        EXPECT_EQ(2U, ids->reserved_count());
    }
}


TEST_F(remote_catalog_manager_test, fetch_exceptions_preserve_publication_and_release_the_name) {
    ASSERT_EQ(status::ok, manager.create({"existing", "worker"}, channels, diagnostic));
    auto existing = manager.find_catalog("existing");
    std::vector<std::function<void()>> exceptions{
        [] { throw std::runtime_error{"fetch failed"}; },
        [] { throw std::bad_alloc{}; },
        [] { throw 1; },
    };
    // Covers successful diagnostic formatting only; allocation failure inside the catch and partial ID insertion
    // are verified by code review, not injected here.
    for (std::size_t i = 0; i < exceptions.size(); ++i) {
        SCOPED_TRACE(i);
        auto before = manager.snapshot();
        fetcher->before_fetch = exceptions[i];
        EXPECT_EQ(status::err_unknown, manager.create({"candidate", "worker"}, channels, diagnostic));
        EXPECT_EQ(before, manager.snapshot());
        EXPECT_EQ(existing, manager.find_catalog("existing"));
        EXPECT_FALSE(manager.find_catalog("candidate"));
        if (i < 2) {
            EXPECT_EQ(0U, diagnostic.find("remote catalog construction failed: "));
        } else {
            EXPECT_EQ("remote catalog construction threw an unknown exception", diagnostic);
        }
        fetcher->before_fetch = {};
        ASSERT_EQ(status::ok, manager.create({"candidate", "worker"}, channels, diagnostic)) << diagnostic;
        ASSERT_EQ(status::ok, manager.drop("candidate"));
    }
}

} // namespace jogasaki::testing
