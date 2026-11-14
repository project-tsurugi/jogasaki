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

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/type/primitive.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/variable/configurable_provider.h>

#include <jogasaki/dist/symbol_locations.h>
#include <jogasaki/dist/snapshot_remote_catalog_fetcher.h>
#include <jogasaki/dist/remote_catalog_information.h>

namespace jogasaki::testing {
namespace {
using namespace dist;
// Detect callback lookups before the configurable provider reacquires its shared mutex.
class non_reentrant_storage final : public yugawara::storage::configurable_provider {
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

TEST(symbol_locations_test, uses_callback_relation_without_recursive_lookup) {
    auto storage = std::make_shared<non_reentrant_storage>();
    auto table = storage->add_table({"sales", {{"sale_number", takatori::type::int8{}}}});
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public", storage});
    auto catalog = std::make_shared<yugawara::schema::catalog>("branch", std::nullopt, schemas);
    auto snapshot = std::make_shared<remote_catalog_snapshot>(
        remote_catalog_snapshot::catalogs_type{catalog},
        remote_catalog_snapshot::locations_type{{table.get(), "worker"}});
    auto registry = std::make_shared<location_registry>();
    symbol_locations query{snapshot, registry};
    auto locations = query.find(*table);
    ASSERT_EQ(1U, locations.size());
    EXPECT_EQ("worker", query.of(*locations.begin()).name);
    EXPECT_FALSE(storage->callback_active);
}

TEST(symbol_locations_test, registry_ids_are_stable_and_unknown_ids_fail) {
    location_registry registry{};
    EXPECT_EQ(0, registry.of(0).id);
    auto first = registry.register_server("first");
    auto second = registry.register_server("second");
    EXPECT_NE(0, first);
    EXPECT_NE(first, second);
    EXPECT_EQ(first, registry.register_server("first"));
    EXPECT_THROW(registry.of(999), std::out_of_range);
    EXPECT_THROW(registry.register_server("local"), std::invalid_argument);
    EXPECT_THROW(registry.register_server(""), std::invalid_argument);
}
TEST(symbol_locations_test, same_names_use_identity_and_snapshot_keeps_catalog_alive) {
    auto local = std::make_shared<yugawara::storage::configurable_provider>();
    auto remote = std::make_shared<yugawara::storage::configurable_provider>();
    auto local_table = local->add_table({"t", {{"id", takatori::type::int8{}}}});
    auto remote_table = remote->add_table({"t", {{"id", takatori::type::int8{}}}});
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public", remote});
    auto catalog = std::make_shared<yugawara::schema::catalog>("remote", std::nullopt, schemas);
    std::weak_ptr<yugawara::schema::catalog> weak = catalog;
    auto snapshot = std::make_shared<remote_catalog_snapshot>(
        remote_catalog_snapshot::catalogs_type{catalog},
        remote_catalog_snapshot::locations_type{{remote_table.get(), "worker"}});
    auto registry = std::make_shared<location_registry>();
    symbol_locations query{snapshot, registry, local};
    auto remote_id = *query.find(*remote_table).begin();
    EXPECT_NE(0, remote_id);
    EXPECT_EQ("worker", query.of(remote_id).name);
    EXPECT_EQ(symbol_locations::id_set_type{0}, query.find(*local_table));
    yugawara::storage::table foreign{"t", {{"id", takatori::type::int8{}}}};
    EXPECT_TRUE(query.find(foreign).empty());
    EXPECT_EQ(catalog.get(), query.external_catalogs().front().get());
    snapshot.reset(); catalog.reset();
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(remote_id, *query.find(*remote_table).begin());
    EXPECT_THROW(query.of(999), std::out_of_range);
}
TEST(symbol_locations_test, only_registered_local_callable_and_variable_identities_are_local) {
    auto functions = std::make_shared<yugawara::function::configurable_provider>();
    auto aggregates = std::make_shared<yugawara::aggregate::configurable_provider>();
    auto variables = std::make_shared<yugawara::variable::configurable_provider>();
    auto function = functions->add(yugawara::function::declaration{1, "f", takatori::type::int8{}, {}});
    auto aggregate = aggregates->add(yugawara::aggregate::declaration{1, "a", takatori::type::int8{}, {}, true});
    auto variable = variables->add(yugawara::variable::declaration{"v", takatori::type::int8{}});
    auto snapshot = std::make_shared<remote_catalog_snapshot>();
    auto registry = std::make_shared<location_registry>();
    symbol_locations query{snapshot, registry, {}, functions, aggregates, variables};
    auto local = symbol_locations::id_set_type{0};
    EXPECT_EQ(local, query.find(*function));
    EXPECT_EQ(local, query.find(*aggregate));
    EXPECT_EQ(local, query.find(*variable));
    yugawara::function::declaration foreign_function{1, "f", takatori::type::int8{}, {}};
    yugawara::aggregate::declaration foreign_aggregate{1, "a", takatori::type::int8{}, {}, true};
    yugawara::variable::declaration foreign_variable{"v", takatori::type::int8{}};
    EXPECT_TRUE(query.find(foreign_function).empty());
    EXPECT_TRUE(query.find(foreign_aggregate).empty());
    EXPECT_TRUE(query.find(foreign_variable).empty());
}
TEST(symbol_locations_test, missing_remote_placement_is_rejected) {
    auto storage = std::make_shared<yugawara::storage::configurable_provider>();
    storage->add_table({"t", {{"id", takatori::type::int8{}}}});
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public", storage});
    auto catalog = std::make_shared<yugawara::schema::catalog>("remote", std::nullopt, schemas);
    auto snapshot = std::make_shared<remote_catalog_snapshot>(remote_catalog_snapshot::catalogs_type{catalog});
    auto registry = std::make_shared<location_registry>();
    EXPECT_THROW((symbol_locations{snapshot, registry}), std::invalid_argument);
}
} // namespace
TEST(snapshot_remote_catalog_fetcher_test, enumerates_without_recursive_provider_lookup) {
    auto storage = std::make_shared<non_reentrant_storage>();
    auto table = storage->add_table({"sales", {{"sale_number", takatori::type::int8{}}}});
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public", storage});
    auto catalog = std::make_shared<yugawara::schema::catalog>("branch", std::nullopt, schemas);
    auto snapshot = std::make_shared<dist::remote_catalog_snapshot>(
        dist::remote_catalog_snapshot::catalogs_type{catalog},
        dist::remote_catalog_snapshot::locations_type{{table.get(), "worker"}});
    dist::snapshot_remote_catalog_fetcher fetcher{snapshot};
    auto references = fetcher.references();
    ASSERT_EQ(1U, references.size());
    EXPECT_EQ("branch", references.front().catalog_name);
    EXPECT_EQ("worker", references.front().remote_server_name);
    dist::remote_catalog_information output{};
    std::string diagnostic{};
    ASSERT_EQ(status::ok, fetcher.fetch(references.front(), {}, {}, output, diagnostic)) << diagnostic;
    ASSERT_EQ(1U, output.tables.size());
    EXPECT_EQ("sales", output.tables.front().name);
    EXPECT_FALSE(storage->callback_active);
}

} // namespace jogasaki::testing
