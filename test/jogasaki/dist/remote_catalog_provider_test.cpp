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
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <optional>
#include <utility>
#include <takatori/util/reference_vector.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/table.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/storage/configurable_provider.h>

#include <unistd.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <takatori/type/int.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/provider.h>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/remote_server_channel_manager.h>
#include <jogasaki/plan/compiler_context.h>

namespace jogasaki::testing {
using namespace dist;
using json = nlohmann::json;

class remote_catalog_provider_test : public ::testing::Test {
protected:
    std::filesystem::path path{};
    json fixture{};
    std::vector<remote_server_configuration> servers{{"worker", "dns:///localhost:12345", false, root_certificate_source::none, {}}};
    std::shared_ptr<remote_catalog_snapshot const> result{};
    std::string diagnostic{};

    void SetUp() override {
        path = std::filesystem::temp_directory_path() / ("jogasaki-catalog-" + std::to_string(::getpid()) + ".json");
        fixture = json::parse(R"fixture({
  "version": 1,
  "catalogs": [{
    "name": "remote_catalog",
    "server": "worker",
    "schemas": [{
      "name": "public",
      "tables": [{
        "name": "t",
        "columns": [
          {"name": "c0", "type": "bigint", "nullable": false},
          {"name": "c1", "type": "bigint", "nullable": true}
        ],
        "primary_key": ["c0"]
      }]
    }]
  }]
}
)fixture");
    }
    void TearDown() override { std::filesystem::remove(path); }
    status load() {
        std::ofstream{path} << fixture.dump();
        return load_remote_catalogs({remote_catalog_source::fixture, path.string()}, servers, result, diagnostic);
    }
    json& table() { return fixture["catalogs"][0]["schemas"][0]["tables"][0]; }
};

TEST_F(remote_catalog_provider_test, default_is_disabled) {
    ASSERT_EQ(status::ok, load_remote_catalogs({}, servers, result, diagnostic));
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->catalogs().empty());
}

TEST_F(remote_catalog_provider_test, rpc_without_transport_is_explicit_error) {
    ASSERT_EQ(status::err_not_implemented, load_remote_catalogs({remote_catalog_source::rpc, {}}, servers, result, diagnostic));
    EXPECT_FALSE(result);
    EXPECT_FALSE(diagnostic.empty());
}

class failing_provider : public remote_catalog_provider {
    status load(std::vector<remote_server_configuration> const&,
        std::shared_ptr<remote_catalog_snapshot const>& result, std::string& diagnostic) override {
        result = std::make_shared<remote_catalog_snapshot>();
        diagnostic = "transport unavailable";
        return status::err_io_error;
    }
};

TEST_F(remote_catalog_provider_test, transport_failure_never_publishes_or_falls_back) {
    result = std::make_shared<remote_catalog_snapshot>();
    failing_provider provider{};
    ASSERT_EQ(status::err_io_error, load_remote_catalogs({remote_catalog_source::rpc, {}}, servers, result, diagnostic, &provider));
    EXPECT_FALSE(result);
    EXPECT_EQ("transport unavailable", diagnostic);
}

class supplied_provider : public remote_catalog_provider {
public:
    std::shared_ptr<remote_catalog_snapshot const> snapshot{};
    bool throws{};
    status load(std::vector<remote_server_configuration> const&, std::shared_ptr<remote_catalog_snapshot const>& result, std::string&) override {
        if (throws) throw std::runtime_error("transport exception");
        result = snapshot;
        return status::ok;
    }
};
TEST_F(remote_catalog_provider_test, rpc_snapshot_rejects_missing_unknown_and_unowned_placements) {
    if (!distributed_query_fixtures_available()) GTEST_SKIP();
    ASSERT_EQ(status::ok, load()) << diagnostic;
    auto catalogs = result->catalogs();
    auto table = catalogs[0]->schema_provider().find("public")->storage_provider().find_table("t");
    yugawara::storage::table foreign{"foreign", {}};
    for (auto const& locations : std::vector<remote_catalog_snapshot::locations_type>{
        {}, {{table.get(), "unknown"}}, {{table.get(), "worker"}, {&foreign, "worker"}}}) {
        supplied_provider provider{};
        provider.snapshot = std::make_shared<remote_catalog_snapshot>(catalogs, locations);
        EXPECT_EQ(status::err_invalid_argument, load_remote_catalogs({remote_catalog_source::rpc, {}}, servers, result, diagnostic, &provider));
        EXPECT_FALSE(result); EXPECT_FALSE(diagnostic.empty());
    }
}
TEST_F(remote_catalog_provider_test, rpc_snapshot_publication_preserves_ownership) {
    if (!distributed_query_fixtures_available()) GTEST_SKIP();
    ASSERT_EQ(status::ok, load()) << diagnostic;
    auto table = result->catalogs()[0]->schema_provider().find("public")->storage_provider().find_table("t");
    supplied_provider provider{}; provider.snapshot = result;
    ASSERT_EQ(status::ok, load_remote_catalogs({remote_catalog_source::rpc, {}}, servers, result, diagnostic, &provider));
    provider.snapshot.reset();
    EXPECT_EQ("worker", *result->remote_server(*table));
}
TEST_F(remote_catalog_provider_test, provider_success_without_snapshot_and_exception_publish_nothing) {
    supplied_provider provider{};
    EXPECT_EQ(status::err_invalid_state, load_remote_catalogs({remote_catalog_source::rpc, {}}, servers, result, diagnostic, &provider));
    EXPECT_FALSE(result);
    provider.throws = true;
    EXPECT_EQ(status::err_io_error, load_remote_catalogs({remote_catalog_source::rpc, {}}, servers, result, diagnostic, &provider));
    EXPECT_FALSE(result); EXPECT_NE(std::string::npos, diagnostic.find("transport exception"));
}
TEST_F(remote_catalog_provider_test, fixture_path_cannot_enable_rpc_dummy) {
    EXPECT_EQ(status::err_invalid_argument,
        load_remote_catalogs({remote_catalog_source::rpc, path.string()}, servers, result, diagnostic));
}

TEST_F(remote_catalog_provider_test, build_switch_controls_fixture_availability) {
    auto st = load();
    EXPECT_EQ(distributed_query_fixtures_available() ? status::ok : status::err_unsupported, st);
    EXPECT_EQ(distributed_query_fixtures_available(), static_cast<bool>(result));
}

TEST_F(remote_catalog_provider_test, catalogs_use_compiler_types_and_table_identity) {
    if (!distributed_query_fixtures_available()) { GTEST_SKIP(); }
    ASSERT_EQ(status::ok, load()) << diagnostic;
    ASSERT_EQ(1, result->catalogs().size());
    auto schema = result->catalogs().front()->schema_provider().find("public");
    ASSERT_TRUE(schema);
    auto table = schema->storage_provider().find_table("t");
    ASSERT_TRUE(table);
    ASSERT_EQ(2, table->columns().size());
    EXPECT_EQ(takatori::type::int8::tag, table->columns()[0].type().kind());
    EXPECT_FALSE(table->columns()[0].criteria().nullity().nullable());
    ASSERT_NE(nullptr, result->remote_server(*table));
    EXPECT_EQ("worker", *result->remote_server(*table));
    yugawara::storage::table local{"t", {}};
    EXPECT_EQ(nullptr, result->remote_server(local));
    EXPECT_TRUE(schema->storage_provider().find_index("t"));
    plan::compiler_context context{};
    context.remote_catalogs(result);
    result.reset();
    EXPECT_EQ("worker", *context.remote_catalogs()->remote_server(*table));
}

TEST_F(remote_catalog_provider_test, same_table_name_in_two_catalogs_has_distinct_location) {
    if (!distributed_query_fixtures_available()) { GTEST_SKIP(); }
    servers.emplace_back(remote_server_configuration{"second", "dns:///localhost:12346", false, root_certificate_source::none, {}});
    auto second = fixture["catalogs"][0]; second["name"] = "other_catalog"; second["server"] = "second";
    fixture["catalogs"].push_back(second);
    ASSERT_EQ(status::ok, load()) << diagnostic;
    auto first = result->catalogs()[0]->schema_provider().find("public")->storage_provider().find_table("t");
    auto other = result->catalogs()[1]->schema_provider().find("public")->storage_provider().find_table("t");
    EXPECT_NE(first.get(), other.get());
    EXPECT_EQ("worker", *result->remote_server(*first));
    EXPECT_EQ("second", *result->remote_server(*other));
}

TEST_F(remote_catalog_provider_test, fixture_preserves_explicit_zero_and_missing_definition_ids) {
    if (!distributed_query_fixtures_available()) GTEST_SKIP();
    for (bool present : {false, true}) {
        if (present) {
            table()["definition_id"] = 0;
            table()["index_definition_id"] = 0;
        }
        ASSERT_EQ(status::ok, load()) << diagnostic;
        auto schema = result->catalogs()[0]->schema_provider().find("public");
        auto table = schema->storage_provider().find_table("t");
        auto index = schema->storage_provider().find_index("t");
        ASSERT_TRUE(table);
        ASSERT_TRUE(index);
        ASSERT_TRUE(result->remote_definition(*table));
        ASSERT_TRUE(result->remote_definition(*index));
        EXPECT_EQ(present, result->remote_definition(*table)->remote_definition_id.has_value());
        EXPECT_EQ(present, result->remote_definition(*index)->remote_definition_id.has_value());
        if (present) EXPECT_EQ(std::optional<std::uint64_t>{0}, result->remote_definition(*index)->remote_definition_id);
    }
}

TEST_F(remote_catalog_provider_test, rejects_invalid_metadata_atomically) {
    if (!distributed_query_fixtures_available()) { GTEST_SKIP(); }
    auto original = fixture;
    std::vector<json> invalid{};
    fixture["version"] = 2; invalid.push_back(fixture); fixture = original;
    fixture["catalogs"][0]["server"] = "unknown"; invalid.push_back(fixture); fixture = original;
    fixture["catalogs"].push_back(fixture["catalogs"][0]); invalid.push_back(fixture); fixture = original;
    table()["columns"][1]["name"] = "c0"; invalid.push_back(fixture); fixture = original;
    table()["columns"][0]["type"] = "unsupported"; invalid.push_back(fixture); fixture = original;
    table()["columns"][0]["nullable"] = true; invalid.push_back(fixture); fixture = original;
    table()["primary_key"] = json::array({"unknown"}); invalid.push_back(fixture); fixture = original;
    table()["primary_key"] = json::array(); invalid.push_back(fixture); fixture = original;
    table()["definition_id"] = -1; invalid.push_back(fixture); fixture = original;
    table()["index_definition_id"] = "1257"; invalid.push_back(fixture); fixture = original;
    table()["columns"] = json::array(); invalid.push_back(fixture);
    for (auto const& entry : invalid) {
        fixture = entry;
        result = std::make_shared<remote_catalog_snapshot>();
        EXPECT_EQ(status::err_invalid_argument, load());
        EXPECT_FALSE(result);
        EXPECT_FALSE(diagnostic.empty());
    }
}

TEST_F(remote_catalog_provider_test, rejects_unsupported_catalog_columns_without_partial_publication) {
    if (!distributed_query_fixtures_available()) { GTEST_SKIP(); }
    for (auto const* type : {"int1", "int2", "tinyint", "smallint", "boolean", "bit", "interval", "datetime_interval"}) {
        ASSERT_EQ(status::ok, load());
        ASSERT_TRUE(result);
        table()["columns"][1]["type"] = type;
        EXPECT_EQ(status::err_invalid_argument, load());
        EXPECT_FALSE(result);
        EXPECT_FALSE(diagnostic.empty());
        table()["columns"][1]["type"] = "bigint";
    }
}

TEST_F(remote_catalog_provider_test, missing_or_malformed_file_is_error) {
    if (!distributed_query_fixtures_available()) { GTEST_SKIP(); }
    EXPECT_EQ(status::err_io_error, load_remote_catalogs({remote_catalog_source::fixture, path.string()}, servers, result, diagnostic));
    std::ofstream{path} << "not json";
    EXPECT_EQ(status::err_invalid_argument, load_remote_catalogs({remote_catalog_source::fixture, path.string()}, servers, result, diagnostic));
    EXPECT_FALSE(result);
}

TEST_F(remote_catalog_provider_test, configuration_requires_explicit_source_and_build) {
    std::stringstream ss{"[distributed_query_coordinator]\nenabled=true\n[distributed_query_client]\ncatalog_source=fixture\nfixture_path=" + path.string() + "\nname=worker\nuri=dns:///localhost:12345\n"};
    tateyama::api::configuration::whole cfg{ss, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_client]\nname=\nuri=\ncatalog_source=disabled\nfixture_path=\n"};
    auto converted = api::resource::convert_config(cfg);
    EXPECT_EQ(distributed_query_fixtures_available(), static_cast<bool>(converted));
    if (converted) {
        EXPECT_EQ(remote_catalog_source::fixture, converted->distributed_query_catalog().source);
        EXPECT_EQ(path.string(), converted->distributed_query_catalog().fixture_path);
        ASSERT_EQ(1, converted->distributed_query_remote_servers().size());
    }
}

TEST_F(remote_catalog_provider_test, rejects_unknown_source_and_implicit_fixture) {
    for (auto const& settings : {"catalog_source=unknown\n", "fixture_path=/tmp/catalog.json\n", "catalog_source=fixture\n"}) {
        std::stringstream ss{std::string{"[distributed_query_coordinator]\nenabled=true\n[distributed_query_client]\n"} + settings};
        tateyama::api::configuration::whole cfg{ss, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_client]\nname=\nuri=\ncatalog_source=disabled\nfixture_path=\n"};
        EXPECT_FALSE(api::resource::convert_config(cfg));
    }
}

TEST_F(remote_catalog_provider_test, channel_reuse_and_stop_do_not_invalidate_borrowed_channel) {
    remote_server_channel_manager manager{};
    EXPECT_FALSE(manager.find_channel("worker"));
    ASSERT_EQ(status::ok, manager.start(servers));
    auto channel = manager.find_channel("worker");
    ASSERT_TRUE(channel);
    EXPECT_EQ(channel, manager.find_channel("worker"));
    EXPECT_FALSE(manager.find_channel("unknown"));
    EXPECT_EQ(status::err_invalid_state, manager.start(servers));
    manager.stop(); manager.stop();
    EXPECT_FALSE(manager.find_channel("worker"));
    EXPECT_TRUE(channel);
}

TEST_F(remote_catalog_provider_test, invalid_channel_configuration_can_be_corrected_before_start) {
    remote_server_channel_manager manager{};
    auto duplicate = servers; duplicate.push_back(servers.front());
    EXPECT_EQ(status::err_invalid_argument, manager.start(duplicate));
    EXPECT_FALSE(manager.find_channel("worker"));
    ASSERT_EQ(status::ok, manager.start(servers));
}

TEST_F(remote_catalog_provider_test, reserved_local_server_is_rejected_without_changing_channels) {
    dist::remote_server_channel_manager manager{};
    EXPECT_EQ(status::err_invalid_argument, manager.start({
        {"local", "dns:///localhost:12345", false, dist::root_certificate_source::none, {}}}));
    EXPECT_FALSE(manager.find_channel("local"));
    EXPECT_EQ(status::ok, manager.start({
        {"worker", "dns:///localhost:12345", false, dist::root_certificate_source::none, {}}}));
    EXPECT_TRUE(manager.find_channel("worker"));
}



namespace {
class validation_storage final : public yugawara::storage::configurable_provider {
public:
    mutable bool callback_active{};
    std::shared_ptr<yugawara::storage::relation const> find_relation(std::string_view name) const override {
        if (callback_active) throw std::logic_error{"recursive provider lookup"};
        return yugawara::storage::configurable_provider::find_relation(name);
    }
    void each_relation(relation_consumer_type consumer) const override {
        yugawara::storage::configurable_provider::each_relation([&](auto name, auto const& relation) {
            callback_active = true;
            try { consumer(name, relation); } catch (...) { callback_active = false; throw; }
            callback_active = false;
        });
    }
};

std::shared_ptr<remote_catalog_snapshot> validation_snapshot(
    std::shared_ptr<validation_storage> const& storage,
    remote_catalog_snapshot::locations_type locations) {
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public", storage});
    auto catalog = std::make_shared<yugawara::schema::catalog>("branch", std::nullopt, schemas);
    return std::make_shared<remote_catalog_snapshot>(
        remote_catalog_snapshot::catalogs_type{catalog}, std::move(locations));
}
} // namespace

TEST_F(remote_catalog_provider_test, validates_callback_relation_without_recursive_lookup) {
    auto storage = std::make_shared<validation_storage>();
    auto table = std::make_shared<yugawara::storage::table>("sales",
        takatori::util::reference_vector<yugawara::storage::column>{});
    storage->add_table(table);
    auto snapshot = validation_snapshot(storage, {{table.get(), "worker"}});
    EXPECT_TRUE(snapshot->validation_error(servers).empty());
    supplied_provider provider{};
    provider.snapshot = snapshot;
    ASSERT_EQ(status::ok, load_remote_catalogs({remote_catalog_source::rpc, {}}, servers,
        result, diagnostic, &provider)) << diagnostic;
    EXPECT_EQ(snapshot, result);
    ASSERT_NE(nullptr, result->remote_server(*table));
    EXPECT_EQ("worker", *result->remote_server(*table));
}

TEST_F(remote_catalog_provider_test, validation_rejects_missing_unknown_and_unowned_placements_without_fixture) {
    auto storage = std::make_shared<validation_storage>();
    auto table = std::make_shared<yugawara::storage::table>("sales",
        takatori::util::reference_vector<yugawara::storage::column>{});
    storage->add_table(table);
    auto foreign = std::make_shared<yugawara::storage::table>("foreign",
        takatori::util::reference_vector<yugawara::storage::column>{});
    std::vector<remote_catalog_snapshot::locations_type> invalid{
        {}, {{table.get(), "unknown"}}, {{table.get(), "worker"}, {foreign.get(), "worker"}}};
    for (std::size_t i = 0; i < invalid.size(); ++i) {
        supplied_provider provider{};
        provider.snapshot = validation_snapshot(storage, invalid[i]);
        result = std::make_shared<remote_catalog_snapshot>();
        EXPECT_EQ(status::err_invalid_argument, load_remote_catalogs({remote_catalog_source::rpc, {}},
            servers, result, diagnostic, &provider));
        EXPECT_FALSE(result);
        EXPECT_EQ(i == 2 ? "remote placement refers to an unowned table"
                         : "missing or unknown remote table placement", diagnostic);
    }
}

} // namespace jogasaki::testing
