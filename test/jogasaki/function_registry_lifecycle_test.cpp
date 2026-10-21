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
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/executor/function/function_registry.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/function/udf_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/udf/data/udf_any_sequence_stream.h>
#include <jogasaki/udf/descriptor_impl.h>
#include <jogasaki/udf/generic_record_impl.h>
#include <jogasaki/udf/udf_loader.h>
#include <jogasaki/utils/finally.h>

namespace jogasaki::testing {
namespace {
using namespace executor::function;
constexpr std::size_t udf_id = 19999;

bool library_loaded() {
    auto* handle = dlopen(UDF_LIFECYCLE_PLUGIN_PATH, RTLD_NOW | RTLD_NOLOAD);
    if (!handle) { return false; }
    dlclose(handle);
    return true;
}

class descriptor_api : public plugin::udf::plugin_api {
public:
    std::vector<plugin::udf::package_descriptor*> const& packages() const noexcept override { return packages_; }
private:
    plugin::udf::column_descriptor_impl column_{0, "value", plugin::udf::type_kind::int8};
    plugin::udf::record_descriptor_impl input_{"Input", {}};
    plugin::udf::record_descriptor_impl output_{"Output", {&column_}};
    plugin::udf::function_descriptor_impl function_{0, "lifecycle", plugin::udf::function_kind::unary, &input_, &output_};
    plugin::udf::service_descriptor_impl service_{0, "Service", {&function_}};
    plugin::udf::package_descriptor_impl package_{"lifecycle", "lifecycle.proto", {0, 5, 0}, {&service_}};
    std::vector<plugin::udf::package_descriptor*> packages_{&package_};
};

class observing_stream : public plugin::udf::generic_record_stream {
public:
    observing_stream(std::weak_ptr<int> owner, bool& alive) : owner_(std::move(owner)), alive_(alive) {}
    observing_stream(observing_stream const&) = delete;
    observing_stream& operator=(observing_stream const&) = delete;
    observing_stream(observing_stream&&) = delete;
    observing_stream& operator=(observing_stream&&) = delete;
    ~observing_stream() override { alive_ = !owner_.expired(); }
    status_type try_next(plugin::udf::generic_record&) override { return status_type::end_of_stream; }
    status_type next(plugin::udf::generic_record&, std::optional<std::chrono::milliseconds>) override { return status_type::end_of_stream; }
    void close() override {}
private:
    std::weak_ptr<int> owner_;
    bool& alive_;
};
}

TEST(function_registry_lifecycle_test, loader_release_keeps_live_plugin_objects_callable) {
    test::temporary_folder temporary{};
    temporary.prepare();
    utils::finally cleanup{[&temporary] { temporary.clean(); }};
    auto directory = std::filesystem::path{temporary.path()};
    std::filesystem::create_directories(directory / "deps");
    // The fixture's basename matches its configuration basename.
    std::filesystem::create_symlink(UDF_LIFECYCLE_PLUGIN_PATH, directory / "lifecycle.so");
    std::ofstream{directory / "lifecycle.ini"} << "[udf]\nenabled=true\nendpoint=dns:///127.0.0.1:50051\nsecure=false\ntransport=stream\n";
    global::config_pool(std::make_shared<configuration>());
    plugin::udf::udf_loader loader{};
    auto results = loader.load(directory.string());
    ASSERT_FALSE(results.empty());
    ASSERT_EQ(plugin::udf::load_status::ok, results.back().status());
    ASSERT_EQ(1U, loader.get_plugins().size());
    auto api = std::get<0>(loader.get_plugins().front());
    auto client = std::get<1>(loader.get_plugins().front()).front().client;
    std::weak_ptr<plugin::udf::plugin_api> api_observer = api;
    std::weak_ptr<plugin::udf::generic_client> client_observer = client;
    loader.unload_all();
    EXPECT_TRUE(library_loaded());
    EXPECT_EQ(1U, api->packages().size());
    api.reset();
    EXPECT_TRUE(library_loaded());
    plugin::udf::generic_client_context context{};
    plugin::udf::generic_record_impl request{};
    plugin::udf::generic_record_impl response{};
    client->call(context, {0, 0}, request, response);
    client.reset();
    EXPECT_TRUE(api_observer.expired());
    EXPECT_TRUE(client_observer.expired());
    // Weak observers remain alive: they must not keep plugin code mapped.
    EXPECT_FALSE(library_loaded());
}

TEST(function_registry_lifecycle_test, global_registry_is_available_without_a_database_owner) {
    auto* registry = &global::function_registry();
    {
        api::impl::database unstarted{};
        EXPECT_EQ(registry, &global::function_registry());
    }
    EXPECT_EQ(registry, &global::function_registry());
    EXPECT_EQ(&registry->scalar_functions(), &global::scalar_function_repository());
    EXPECT_EQ(&registry->table_functions(), &global::table_valued_function_repository());
    EXPECT_EQ(&registry->aggregate_functions(), &global::aggregate_function_repository());
    EXPECT_EQ(&registry->incremental_functions(), &global::incremental_aggregate_function_repository());
    EXPECT_EQ(registry->regular_provider(), global::regular_function_provider());
}

TEST(function_registry_lifecycle_test, registry_releases_all_function_kinds) {
    function_registry functions{};
    configuration cfg{};
    functions.initialize(cfg);
    ASSERT_GT(functions.scalar_functions().size(), 0U);
    ASSERT_GT(functions.aggregate_functions().size(), 0U);
    ASSERT_GT(functions.incremental_functions().size(), 0U);
    functions.table_functions().add(udf_id,
        std::make_shared<table_valued_function_info>(table_valued_function_kind::user_defined, table_valued_function_type{}, 0));
    functions.clear();
    EXPECT_EQ(0U, functions.scalar_functions().size());
    EXPECT_EQ(0U, functions.table_functions().size());
    EXPECT_EQ(0U, functions.aggregate_functions().size());
    EXPECT_EQ(0U, functions.incremental_functions().size());

}

TEST(function_registry_lifecycle_test, callable_retains_descriptors_after_plugin_collection_release) {
    auto api = std::make_shared<descriptor_api>();
    std::weak_ptr<descriptor_api> weak = api;
    std::vector<plugin::udf::plugin_entry> plugins{};
    plugins.emplace_back(api, plugin::udf::generic_client_list{{{}, {}}}, std::make_shared<plugin::udf::udf_config>());
    yugawara::function::configurable_provider provider{};
    scalar_function_repository scalar{};
    table_valued_function_repository table{};
    add_udf_functions(provider, scalar, table, plugins);
    ASSERT_EQ(1U, scalar.size());
    plugins.clear();
    api.reset();
    EXPECT_FALSE(weak.expired());
    auto callable = scalar.find(20000)->function_body();
    scalar.clear();
    EXPECT_FALSE(weak.expired());
    callable = {};
    EXPECT_TRUE(weak.expired());
}

TEST(function_registry_lifecycle_test, stream_retains_owner_until_plugin_stream_destruction) {
    mock_memory_resource resource{};
    auto owner = std::make_shared<int>(1);
    std::weak_ptr<int> weak = owner;
    bool alive_during_destruction = false;
    auto source = std::make_unique<observing_stream>(weak, alive_during_destruction);
    auto stream = std::make_unique<udf::data::udf_any_sequence_stream>(
        std::move(source), std::vector<udf::data::udf_wire_kind>{}, &resource, owner);
    owner.reset();
    EXPECT_FALSE(weak.expired());
    stream.reset();
    EXPECT_TRUE(alive_during_destruction);
    EXPECT_TRUE(weak.expired());
}

class database_function_lifecycle_test : public ::testing::Test, public api_test_base {
protected:
    bool to_explain() override { return false; }
    void SetUp() override { db_setup(); }
    void TearDown() override { db_teardown(); }
    void register_udf(std::shared_ptr<int> owner) {
        global::scalar_function_repository().add(udf_id,
            std::make_shared<scalar_function_info>(scalar_function_kind::user_defined,
                [owner](auto&, auto) { return data::any{std::in_place_type<std::int32_t>, *owner}; }, 0));
        global::table_valued_function_repository().add(udf_id,
            std::make_shared<table_valued_function_info>(table_valued_function_kind::user_defined,
                table_valued_function_type{}, 0));
    }
};

TEST_F(database_function_lifecycle_test, unstarted_database_destruction_preserves_active_registrations) {
    auto& functions = global::function_registry();
    auto scalar_count = functions.scalar_functions().size();
    ASSERT_GT(scalar_count, 0U);
    {
        api::impl::database unstarted{};
    }
    EXPECT_EQ(scalar_count, functions.scalar_functions().size());
    EXPECT_EQ(db_impl()->aggregate_functions(), functions.aggregate_provider());
}

TEST_F(database_function_lifecycle_test, stop_releases_registration_and_resources) {
    auto owner = std::make_shared<int>(1);
    std::weak_ptr<int> old = owner;
    register_udf(owner);
    owner.reset();
    EXPECT_FALSE(old.expired());
    auto* registry = &global::function_registry();
    EXPECT_EQ(db_impl()->aggregate_functions(), registry->aggregate_provider());
    std::weak_ptr<yugawara::function::configurable_provider> previous_provider = registry->regular_provider();
    ASSERT_EQ(status::ok, db_->stop());
    EXPECT_TRUE(previous_provider.expired());
    EXPECT_EQ(registry, &global::function_registry());
    EXPECT_TRUE(old.expired());
    EXPECT_EQ(0U, global::function_registry().scalar_functions().size());
    EXPECT_EQ(0U, global::function_registry().table_functions().size());
    EXPECT_EQ(0U, global::function_registry().aggregate_functions().size());
    EXPECT_EQ(0U, global::function_registry().incremental_functions().size());
    EXPECT_EQ(nullptr, global::scalar_function_repository().find(udf_id));
    EXPECT_EQ(nullptr, global::table_valued_function_repository().find(udf_id));

}

TEST_F(database_function_lifecycle_test, stop_start_restores_builtin_functions_and_recovered_rows) {
    execute_statement("CREATE TABLE lifecycle_restart (id INT PRIMARY KEY, v INT)");
    execute_statement("INSERT INTO lifecycle_restart VALUES (1, -10), (2, -20)");
    auto* registry = &global::function_registry();
    for (std::size_t i = 0; i < 2; ++i) {
        ASSERT_EQ(status::ok, db_->stop());
        EXPECT_EQ(registry, &global::function_registry());
        EXPECT_EQ(0U, global::function_registry().scalar_functions().size());
        EXPECT_EQ(0U, global::function_registry().incremental_functions().size());
        ASSERT_EQ(status::ok, db_->start());
        EXPECT_EQ(registry, &global::function_registry());
        EXPECT_GT(global::function_registry().scalar_functions().size(), 0U);
        EXPECT_GT(global::function_registry().incremental_functions().size(), 0U);
        std::vector<mock::basic_record> rows{};
        execute_query("SELECT ABS(SUM(v)), COUNT(*) FROM lifecycle_restart", rows);
        ASSERT_EQ(1U, rows.size());
        EXPECT_EQ(30, rows[0].get_value<std::int32_t>(0));
        EXPECT_EQ(2, rows[0].get_value<std::int64_t>(1));
    }
}

TEST_F(database_function_lifecycle_test, restart_reuses_process_wide_plugins_despite_directory_change) {
    test::temporary_folder temporary{};
    temporary.prepare();
    utils::finally cleanup{[&temporary] { temporary.clean(); }};
    auto directory = std::filesystem::path{temporary.path()};
    std::filesystem::create_directories(directory / "deps");
    std::filesystem::create_directories(directory / "empty");
    std::filesystem::create_symlink(UDF_LIFECYCLE_PLUGIN_PATH, directory / "lifecycle.so");
    std::ofstream{directory / "lifecycle.ini"} << "[udf]\nenabled=true\nendpoint=dns:///127.0.0.1:50051\nsecure=false\ntransport=stream\n";
    ASSERT_EQ(status::ok, db_->stop());
    auto& functions = global::function_registry();
    // Reset the singleton only for test isolation, after database execution has stopped.
    functions.clear();
    utils::finally reset_registry{[this, &functions] {
        if (db_->stop() == status::ok) { functions.clear(); }
    }};
    auto cfg = db_impl()->configuration();
    cfg->plugin_directory(directory.string());
    ASSERT_EQ(status::ok, db_->start());
    ASSERT_TRUE(library_loaded());
    auto expect_udf_value = [this] {
        std::vector<mock::basic_record> rows{};
        execute_query("SELECT lifecycle_value()", rows);
        ASSERT_EQ(1U, rows.size());
        EXPECT_EQ(42, rows[0].get_value<std::int64_t>(0));
    };
    expect_udf_value();
    cfg->plugin_directory((directory / "empty").string());
    for (std::size_t i = 0; i < 2; ++i) {
        ASSERT_EQ(status::ok, db_->stop());
        EXPECT_EQ(0U, functions.scalar_functions().size());
        EXPECT_TRUE(library_loaded());
        ASSERT_EQ(status::ok, db_->start());
        EXPECT_EQ(&functions, &global::function_registry());
        EXPECT_TRUE(library_loaded());
        EXPECT_GT(functions.scalar_functions().size(), 0U);
        expect_udf_value();
    }
    ASSERT_EQ(status::ok, db_->stop());
    EXPECT_TRUE(library_loaded());
    functions.clear();
    EXPECT_FALSE(library_loaded());
}

}
