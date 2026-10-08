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
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
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
    EXPECT_TRUE(api->packages().empty());
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

TEST(function_registry_lifecycle_test, database_owns_independent_function_registries) {
    auto first = std::make_unique<api::impl::database>();
    api::impl::database second{};
    auto resource = std::make_shared<int>(1);
    std::weak_ptr<int> observer = resource;
    first->functions().scalar_functions().add(udf_id,
        std::make_shared<scalar_function_info>(scalar_function_kind::user_defined,
            [resource](auto&, auto) { return data::any{std::in_place_type<std::int32_t>, *resource}; }, 0));
    resource.reset();
    EXPECT_NE(nullptr, first->functions().scalar_functions().find(udf_id));
    EXPECT_EQ(nullptr, second.functions().scalar_functions().find(udf_id));
    second.functions().clear();
    EXPECT_FALSE(observer.expired());
    first.reset();
    EXPECT_TRUE(observer.expired());
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

TEST_F(database_function_lifecycle_test, global_access_does_not_keep_the_registry_alive) {
    auto database_owner = std::shared_ptr<function_registry>{global::database_impl(), &db_impl()->functions()};
    utils::finally restore{[database_owner] { (void) global::function_registry(database_owner); }};
    auto temporary = std::make_shared<function_registry>();
    std::weak_ptr<function_registry> observer = temporary;
    (void) global::function_registry(temporary);
    temporary.reset();
    EXPECT_TRUE(observer.expired());
    EXPECT_ANY_THROW((void) global::scalar_function_repository());
}

TEST_F(database_function_lifecycle_test, concurrent_registry_binding_and_lookup) {
    auto database_owner = std::shared_ptr<function_registry>{global::database_impl(), &db_impl()->functions()};
    utils::finally restore{[database_owner] { (void) global::function_registry(database_owner); }};
    auto first = std::make_shared<function_registry>();
    auto second = std::make_shared<function_registry>();
    (void) global::function_registry(first);
    std::atomic<bool> ready{false};
    std::thread writer{[&] {
        ready.store(true);
        for (std::size_t i = 0; i < 10000; ++i) {
            (void) global::function_registry(i % 2 == 0 ? first : second);
        }
    }};
    while (!ready.load()) { std::this_thread::yield(); }
    for (std::size_t i = 0; i < 10000; ++i) {
        auto* registry = &global::function_registry();
        EXPECT_TRUE(registry == first.get() || registry == second.get());
    }
    writer.join();
}

TEST_F(database_function_lifecycle_test, stop_releases_registration_and_resources) {
    auto owner = std::make_shared<int>(1);
    std::weak_ptr<int> old = owner;
    register_udf(owner);
    owner.reset();
    EXPECT_FALSE(old.expired());
    ASSERT_EQ(status::ok, db_->stop());
    EXPECT_TRUE(old.expired());
    EXPECT_EQ(0U, db_impl()->functions().scalar_functions().size());
    EXPECT_EQ(0U, db_impl()->functions().table_functions().size());
    EXPECT_EQ(0U, db_impl()->functions().aggregate_functions().size());
    EXPECT_EQ(0U, db_impl()->functions().incremental_functions().size());
    EXPECT_EQ(nullptr, global::scalar_function_repository().find(udf_id));
    EXPECT_EQ(nullptr, global::table_valued_function_repository().find(udf_id));

}

}
