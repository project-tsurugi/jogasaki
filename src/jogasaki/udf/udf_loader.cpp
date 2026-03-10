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

#include <fstream>
#include <google/protobuf/descriptor.pb.h>
#include <dlfcn.h>
#include <filesystem>
#include <glog/logging.h>
#include <iostream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/udf/log/logging_prefix.h>
#include "enum_types.h"
#include "error_info.h"
#include "generic_client_factory.h"
#include "generic_record_impl.h"
#include "udf_config.h"

#include <grpcpp/grpcpp.h>
namespace fs = std::filesystem;
using namespace plugin::udf;
using jogasaki::location_prefix;

namespace {
bool validate_directory(fs::path const& path, std::vector<load_result>& results) {
    if (!fs::exists(path)) {
        results.emplace_back(load_status::path_not_found, path.string(), "Directory not found");
        return false;
    }
    if (!fs::is_directory(path)) {
        results.emplace_back(load_status::path_not_found, path.string(), "Path is not a directory");
        return false;
    }
    return true;
}

std::vector<fs::path> collect_ini_files(fs::path const& dir) {
    std::vector<fs::path> result{};
    for (auto const& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file()) { continue; }

        auto const& path = entry.path();
        if (path.extension() == ".ini") { result.emplace_back(path); }
    }
    std::sort(result.begin(), result.end());
    return result;
}

std::vector<fs::path> collect_desc_pb_files(fs::path const& dir) {
    std::vector<fs::path> result{};
    constexpr std::string_view suffix = ".desc.pb";

    for (auto const& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file()) { continue; }

        auto const& path = entry.path();
        auto const filename = path.filename().string();
        if (filename.size() >= suffix.size() &&
            filename.compare(filename.size() - suffix.size(), suffix.size(), suffix) == 0) {
            result.emplace_back(path);
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}

struct rpc_info {
    std::string proto_file_name;
    std::string service_name;
    std::string full_rpc_path;
};

[[nodiscard]] std::string build_full_rpc_path(
    std::string const& pkg, std::string const& service_name, std::string const& method_name) {
    std::string full_rpc_path;
    full_rpc_path.reserve(pkg.size() + service_name.size() + method_name.size() + 3);

    full_rpc_path += "/";
    if (!pkg.empty()) {
        full_rpc_path += pkg;
        full_rpc_path += ".";
    }
    full_rpc_path += service_name;
    full_rpc_path += "/";
    full_rpc_path += method_name;

    return full_rpc_path;
}

[[nodiscard]] bool insert_rpc_info(std::map<std::string, rpc_info>& seen,
    std::string const& method_name, rpc_info const& current) {
    auto const result = seen.emplace(method_name, current);
    auto const& it = result.first;
    auto const inserted = result.second;

    if (inserted) { return true; }

    LOG_LP(WARNING) << jogasaki::udf::log::prefix << "RPC name duplicated: " << method_name
                    << " first: proto=" << it->second.proto_file_name
                    << " service=" << it->second.service_name << " rpc=" << it->second.full_rpc_path
                    << " second: proto=" << current.proto_file_name
                    << " service=" << current.service_name << " rpc=" << current.full_rpc_path;
    return false;
}

[[nodiscard]] bool validate_rpc_methods_in_file(
    google::protobuf::FileDescriptorSet const& fds, std::map<std::string, rpc_info>& seen) {
    for (auto const& file : fds.file()) {
        auto const& pkg = file.package();
        auto const& proto = file.name();

        for (auto const& service : file.service()) {
            auto const& service_name = service.name();

            for (auto const& method : service.method()) {
                auto const& method_name = method.name();

                rpc_info current;
                current.proto_file_name = proto;
                current.service_name = service_name;
                current.full_rpc_path = build_full_rpc_path(pkg, service_name, method_name);

                if (!insert_rpc_info(seen, method_name, current)) { return false; }
            }
        }
    }
    return true;
}

bool validate_rpc_method_duplicates(std::vector<fs::path> const& desc_files) {
    if (desc_files.empty()) {
        LOG_LP(WARNING) << jogasaki::udf::log::prefix
                        << "no descriptor files found; RPC duplication check skipped";
        return true;
    }

    std::map<std::string, rpc_info> seen{};

    for (auto const& desc_path : desc_files) {
        std::ifstream input(desc_path, std::ios::binary);
        if (!input) {
            LOG_LP(WARNING) << jogasaki::udf::log::prefix
                            << "cannot open descriptor: " << desc_path.string();
            return false;
        }

        google::protobuf::FileDescriptorSet fds;
        if (!fds.ParseFromIstream(&input)) {
            LOG_LP(WARNING) << jogasaki::udf::log::prefix
                            << "failed to parse descriptor: " << desc_path.string();
            return false;
        }

        if (!validate_rpc_methods_in_file(fds, seen)) { return false; }
    }

    return true;
}

} // namespace

[[nodiscard]] std::string const& client_info::default_endpoint() const noexcept { return default_endpoint_; }
[[nodiscard]] bool client_info::default_secure() const noexcept { return default_secure_; }
void client_info::set_default_endpoint(std::string endpoint) { default_endpoint_ = std::move(endpoint); }
void client_info::set_default_secure(bool secure) { default_secure_ = secure; }
std::optional<udf_config> udf_loader::parse_ini(fs::path const& ini_path, std::vector<load_result>& results) {
    try {
        boost::property_tree::ptree pt;
        boost::property_tree::ini_parser::read_ini(ini_path.string(), pt);

        auto enabled_opt = pt.get_optional<std::string>("udf.enabled");
        if(! enabled_opt) {
            results.emplace_back(load_status::ini_invalid, ini_path.string(), "Missing required field: udf.enabled");
            return std::nullopt;
        }

        std::string enabled_str = *enabled_opt;
        std::transform(enabled_str.begin(), enabled_str.end(), enabled_str.begin(), ::tolower);
        bool enabled = true;
        if(enabled_str == "true") {
            enabled = true;
        } else if(enabled_str == "false") {
            enabled = false;
        } else {
            results.emplace_back(
                load_status::ini_invalid,
                ini_path.string(),
                "Invalid value for udf.enabled (must be true or false)"
            );
            return std::nullopt;
        }

        std::string endpoint = std::string(jogasaki::global::config_pool()->endpoint());
        if (auto opt = pt.get_optional<std::string>("udf.endpoint")) { endpoint = *opt; }
        std::string transport = "stream";
        if (auto opt = pt.get_optional<std::string>("udf.transport")) {
            transport = *opt;
            if (transport.empty()) {
                transport = "stream";
            }
        }
        bool secure = jogasaki::global::config_pool()->secure();
        if(auto opt = pt.get_optional<std::string>("udf.secure")) {
            std::string val = *opt;
            std::transform(val.begin(), val.end(), val.begin(), ::tolower);
            if(val == "false") {
                secure = false;
            } else if(val == "true") {
                secure = true;
            } else {
                results.emplace_back(
                    load_status::ini_invalid,
                    ini_path.string(),
                    "Invalid value for udf.secure (must be true or false)"
                );
                return std::nullopt;
            }
        }

        return udf_config(enabled, std::move(endpoint), std::move(transport), secure);

    } catch(std::exception const& e) {
        results.emplace_back(load_status::ini_invalid, ini_path.string(), e.what());
        return std::nullopt;
    }
}

std::vector<load_result> udf_loader::load(std::string_view dir_path) {
    fs::path path(dir_path);
    std::vector<load_result> results{};
    std::vector<std::filesystem::path> ini_files{};
    std::vector<std::filesystem::path> desc_files{};
    if (dir_path.empty()) {
        return {load_result(load_status::path_is_empty, "", "Directory path is empty")};
    }
    if (! validate_directory(path, results)) { return results; }
    ini_files = collect_ini_files(path);
    if (ini_files.empty()) {
        results.emplace_back(load_status::no_ini_files, std::string(dir_path),
            "No .ini files found (UDF disabled)");
        return results;
    }
    desc_files = collect_desc_pb_files(path);
    if (! validate_rpc_method_duplicates(desc_files)) {
        results.emplace_back(load_status::rpc_name_duplicated, std::string(dir_path), "RPC name duplicated");
        return results;
    }
    LOG_LP(INFO) << jogasaki::udf::log::prefix << "No RPC name duplication detected in descriptors";
    for (auto const& ini_path : ini_files) {
        auto udf_config_value = parse_ini(ini_path, results);
        if (! udf_config_value){
            continue;
        }
        if (! udf_config_value->enabled()) {
            results.emplace_back(
                load_status::udf_disabled, ini_path.string(), "UDF disabled in configuration");
            continue;
        }
        auto so_path = ini_path;
        so_path.replace_extension(".so");
        if (!fs::exists(so_path)) {
            continue;
        }
        VLOG(jogasaki::log_trace) << jogasaki::udf::log::prefix << "UDF library found: " << so_path.string()
                                  << " (config: " << ini_path.string() << ")";
        std::string full_path = so_path.string();
        dlerror();
        void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (! handle) {
            const char* err = dlerror();
            results.emplace_back(load_status::dlopen_failed, full_path,
                err ? err : "dlopen failed with unknown error");
            return results;
        }
        auto cfg_sp = std::make_shared<udf_config>(std::move(*udf_config_value));
        auto res = create_api_from_handle(handle, full_path, cfg_sp);
        if (res.status() == load_status::ok) {
            handles_.push_back(handle);
        } else {
            dlclose(handle);
        }
        results.push_back(std::move(res));
    }
    return results;
}

void udf_loader::unload_all() {
    plugins_.clear();
    for (auto* h : handles_) {
        if (h) dlclose(h);
    }
    handles_.clear();
}
load_result udf_loader::create_api_from_handle(
    void* handle,
    std::string const& full_path,
    std::shared_ptr<const udf_config> cfg
) {
    if(! handle) { return {load_status::dlopen_failed, "", "Invalid handle (nullptr)"}; }

    using create_api_func = plugin_api* (*) ();
    using create_factory_func = generic_client_factory* (*) (char const*);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* api_func = reinterpret_cast<create_api_func>(dlsym(handle, "create_plugin_api"));
    if(! api_func) { return {load_status::api_symbol_missing, full_path, "Symbol 'create_plugin_api' not found"}; }

    auto api_uptr = std::unique_ptr<plugin_api>(api_func());
    if(! api_uptr) { return {load_status::api_init_failed, full_path, "Failed to initialize plugin API"}; }
    std::shared_ptr<plugin_api> api_sptr = std::move(api_uptr);
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
    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (cfg->secure()) {
        grpc::SslCredentialsOptions opts;
        creds = grpc::SslCredentials(opts);
        VLOG(jogasaki::log_trace) << jogasaki::udf::log::prefix
                                  << "Creating TLS channel to endpoint: " << cfg->endpoint()
                                  << " (using system root certificates)";
    } else {
        creds = grpc::InsecureChannelCredentials();
        VLOG(jogasaki::log_trace) << jogasaki::udf::log::prefix
                                  << "Creating INSECURE channel to endpoint: " << cfg->endpoint();
    }
    auto channel = grpc::CreateChannel(cfg->endpoint(), creds);
    auto raw_client = factory_ptr->create(channel);
    if(! raw_client) {
        return {load_status::factory_creation_failed, full_path, "Failed to create generic client from factory"};
    }
    plugins_.emplace_back(std::move(api_sptr), std::shared_ptr<generic_client>(raw_client),std::move(cfg));
    return {load_status::ok, full_path, "Loaded successfully"};
}
std::vector<plugin_entry>& udf_loader::get_plugins() noexcept { return plugins_; }

udf_loader::~udf_loader() { unload_all(); }
