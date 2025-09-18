/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "udf_loader.h"
#include "enum_types.h"
#include "error_info.h"
#include "generic_client_factory.h"
#include "generic_record_impl.h"
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <dlfcn.h>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
namespace fs = std::filesystem;
using namespace plugin::udf;

[[nodiscard]] const std::string& client_info::default_url() const noexcept { return default_url_; }
[[nodiscard]] const std::string& client_info::default_auth() const noexcept {
    return default_auth_;
}
void client_info::set_default_url(std::string url) { default_url_ = std::move(url); }
void client_info::set_default_auth(std::string auth) { default_auth_ = std::move(auth); }
load_result udf_loader::load(std::string_view dir_path) {
    fs::path path(dir_path);
    std::vector<fs::path> files_to_load;
    if (fs::is_directory(path)) {
        for (const auto& entry : fs::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".so") {
                files_to_load.push_back(entry.path());
            }
        }
    } else {
        return {load_status::NotRegularFileOrDir, std::string(dir_path), "Path is not a directory"};
    }
    for (const auto& file : files_to_load) {
        std::string full_path = file.string();
        dlerror();
        void* handle    = dlopen(full_path.c_str(), RTLD_NOW | RTLD_GLOBAL);
        const char* err = dlerror();
        if (!handle || err) {
            return {load_status::DLOpenFailed, full_path,
                err ? err : "dlopen failed with unknown error"};
        }
        handles_.emplace_back(handle);
        auto res = create_api_from_handle(handle, full_path);
        if (res.status() != load_status::OK) {
            res.set_file(full_path);
            return res;
        }
    }
    return {load_status::OK, "", ""};
}

void udf_loader::unload_all() {
    plugins_.clear();
    for (void* handle : handles_) {
        if (handle) dlclose(handle);
    }
    handles_.clear();
}
load_result udf_loader::create_api_from_handle(void* handle, const std::string& full_path) {
    if (!handle) { return {load_status::DLOpenFailed, "", "Invalid handle (nullptr)"}; }

    using create_api_func     = plugin_api* (*)();
    using create_factory_func = generic_client_factory* (*)(const char*);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* api_func = reinterpret_cast<create_api_func>(dlsym(handle, "create_plugin_api"));
    if (!api_func) {
        return {load_status::ApiSymbolMissing, "", "Symbol 'create_plugin_api' not found"};
    }

    auto api_ptr = std::unique_ptr<plugin_api>(api_func());
    if (!api_ptr) { return {load_status::ApiInitFailed, "", "Failed to initialize plugin API"}; }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* factory_func = reinterpret_cast<create_factory_func>(
        dlsym(handle, "tsurugi_create_generic_client_factory"));
    if (!factory_func) {
        return {load_status::FactorySymbolMissing, "",
            "Symbol 'tsurugi_create_generic_client_factory' not found"};
    }

    auto factory_ptr = std::unique_ptr<generic_client_factory>(factory_func("Greeter"));
    if (!factory_ptr) {
        return {load_status::FactorySymbolMissing, "",
            "Symbol 'tsurugi_create_generic_client_factory' not found"};
    }
    client_info info;
    std::filesystem::path ini_path = full_path;
    ini_path.replace_extension(".ini");
    if (std::filesystem::exists(ini_path)) {
        boost::property_tree::ptree pt;
        boost::property_tree::ini_parser::read_ini(ini_path.string(), pt);

        if (auto opt = pt.get_optional<std::string>("grpc.url")) { info.set_default_url(*opt); }
        if (auto opt = pt.get_optional<std::string>("grpc.credentials")) {
            info.set_default_auth(*opt);
        }
    }
    auto channel =
        grpc::CreateChannel(std::string("localhost:50051"), grpc::InsecureChannelCredentials());
    auto raw_client = factory_ptr->create(channel);
    if (!raw_client) {
        return {
            load_status::FactoryCreationFailed, "", "Failed to create generic client from factory"};
    }
    plugins_.emplace_back(api_ptr.release(), std::shared_ptr<generic_client>(raw_client));
    return {load_status::OK, "", ""};
}

std::vector<std::tuple<std::shared_ptr<plugin_api>, std::shared_ptr<generic_client>>>&
udf_loader::get_plugins() noexcept {
    return plugins_;
}
udf_loader::~udf_loader() { unload_all(); }
