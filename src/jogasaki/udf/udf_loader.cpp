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

#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "enum_types.h"
#include "error_info.h"
#include "generic_client_factory.h"
#include "generic_record_impl.h"

#include <grpcpp/grpcpp.h>
namespace fs = std::filesystem;
using namespace plugin::udf;

udf_config::udf_config(bool enabled, std::string url, std::string credentials) :
    _enabled(enabled),
    _url(std::move(url)),
    _credentials(std::move(credentials)) {}

bool udf_config::enabled() const noexcept { return _enabled; }
std::string const& udf_config::url() const noexcept { return _url; }
std::string const& udf_config::credentials() const noexcept { return _credentials; }

[[nodiscard]] std::string const& client_info::default_url() const noexcept { return default_url_; }
[[nodiscard]] std::string const& client_info::default_auth() const noexcept { return default_auth_; }
void client_info::set_default_url(std::string url) { default_url_ = std::move(url); }
void client_info::set_default_auth(std::string auth) { default_auth_ = std::move(auth); }

std::optional<udf_config>
udf_loader::parse_ini(fs::path const& ini_path, std::vector<load_result>& results, std::string const& name) {
    try {
        boost::property_tree::ptree pt;
        boost::property_tree::ini_parser::read_ini(ini_path.string(), pt);

        bool enabled = true;
        std::string url;
        std::string credentials;

        if(auto opt = pt.get_optional<std::string>("udf.enabled")) {
            std::string val = *opt;
            std::transform(val.begin(), val.end(), val.begin(), ::tolower);
            if(val == "false") {
                enabled = false;
            } else if(val != "true") {
                results.emplace_back(load_status::ini_so_pair_mismatch, name, "udf.enabled must be true or false");
                return std::nullopt;
            }
        }
        if(auto opt = pt.get_optional<std::string>("udf.url")) {
            url = *opt;
        } else {
            results.emplace_back(load_status::ini_so_pair_mismatch, name, "udf.url must be set");
            return std::nullopt;
        }
        if(auto opt = pt.get_optional<std::string>("udf.credentials")) {
            credentials = *opt;
        } else {
            results.emplace_back(load_status::ini_so_pair_mismatch, name, "udf.credentials must be set");
            return std::nullopt;
        }
        return udf_config(enabled, std::move(url), std::move(credentials));

    } catch(std::exception const& e) {
        results.emplace_back(load_status::ini_invalid, ini_path.string(), e.what());
        return std::nullopt;
    }
}

std::vector<load_result> udf_loader::load(std::string_view dir_path) {

    fs::path path(dir_path);
    std::vector<load_result> results;

    if(! fs::exists(path)) {
        results.emplace_back(load_status::path_not_found, std::string(dir_path), "Directory not found");
        return results;
    }
    if(! fs::is_directory(path)) {
        results.emplace_back(load_status::path_not_found, std::string(dir_path), "Path is not a directory");
        return results;
    }

    std::unordered_map<std::string, std::pair<fs::path, fs::path>> pairs;
    for(auto const& entry: fs::directory_iterator(path)) {
        if(! entry.is_regular_file()) continue;

        auto ext = entry.path().extension();
        auto stem = entry.path().stem().string();
        auto& p = pairs[stem];

        if(ext == ".ini") {
            p.first = entry.path();
        } else if(ext == ".so") {
            p.second = entry.path();
        }
    }

    if(pairs.empty()) {
        results.emplace_back(
            load_status::no_ini_and_so_files,
            std::string(dir_path),
            "No .ini or .so files found (UDF disabled)"
        );
        return results;
    }
    for(auto const& [name, paths]: pairs) {
        const auto& ini_path = paths.first;
        const auto& so_path = paths.second;

        if(ini_path.empty() || so_path.empty()) {
            results.emplace_back(load_status::ini_so_pair_mismatch, name, "Missing paired .ini or .so file");
            return results;
        }
        auto udf_config_value = parse_ini(ini_path, results, name);
        if(! udf_config_value.has_value()) { return results; }
        if(! udf_config_value->enabled()) {
            results.emplace_back(load_status::udf_disabled, name, "UDF disabled in configuration");
            continue;
        }
        std::string full_path = so_path.string();
        dlerror();
        void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        const char* err = dlerror();
        if(! handle || err) {
            results.emplace_back(load_status::dlopen_failed, full_path, err ? err : "dlopen failed with unknown error");
            return results;
        }
        auto res = create_api_from_handle(handle, full_path, udf_config_value->url(), udf_config_value->credentials());
        results.push_back(std::move(res));
    }
    return results;
}

void udf_loader::unload_all() {}
load_result udf_loader::create_api_from_handle(
    void* handle,
    std::string const& full_path,
    std::string const& url,
    std::string const& credentials
) {
    if(! handle) { return {load_status::dlopen_failed, "", "Invalid handle (nullptr)"}; }

    using create_api_func = plugin_api* (*) ();
    using create_factory_func = generic_client_factory* (*) (char const*);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* api_func = reinterpret_cast<create_api_func>(dlsym(handle, "create_plugin_api"));
    if(! api_func) { return {load_status::api_symbol_missing, full_path, "Symbol 'create_plugin_api' not found"}; }

    auto api_ptr = std::unique_ptr<plugin_api>(api_func());
    if(! api_ptr) { return {load_status::api_init_failed, full_path, "Failed to initialize plugin API"}; }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* factory_func = reinterpret_cast<create_factory_func>(dlsym(handle, "tsurugi_create_generic_client_factory"));
    if(! factory_func) {
        return {
            load_status::factory_symbol_missing,
            full_path,
            "Symbol 'tsurugi_create_generic_client_factory' not found"};
    }

    auto factory_ptr = std::unique_ptr<generic_client_factory>(factory_func("Greeter"));
    if(! factory_ptr) {
        return {
            load_status::factory_symbol_missing,
            full_path,
            "Symbol 'tsurugi_create_generic_client_factory' not found"};
    }
    if(credentials != "insecure") {
        return {load_status::ini_invalid, full_path, "Currently, only 'insecure' credentials are supported"};
    }
    auto channel = grpc::CreateChannel(url, grpc::InsecureChannelCredentials());
    auto raw_client = factory_ptr->create(channel);
    if(! raw_client) {
        return {load_status::factory_creation_failed, full_path, "Failed to create generic client from factory"};
    }
    plugins_.emplace_back(api_ptr.release(), std::shared_ptr<generic_client>(raw_client));
    return {load_status::ok, full_path, "Loaded successfully"};
}

std::vector<std::tuple<std::shared_ptr<plugin_api>, std::shared_ptr<generic_client>>>&
udf_loader::get_plugins() noexcept {
    return plugins_;
}
udf_loader::~udf_loader() { unload_all(); }
