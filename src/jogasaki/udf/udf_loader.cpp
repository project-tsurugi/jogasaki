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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <dlfcn.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>

#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/udf/descriptor/descriptor_analyzer.h>
#include <jogasaki/udf/descriptor/validation/message_duplicate_validator.h>
#include <jogasaki/udf/descriptor/validation/rpc_duplicate_validator.h>
#include <jogasaki/udf/log/logging_prefix.h>

#include "enum_types.h"
#include "error_info.h"
#include "generic_client_factory.h"
#include "generic_record_impl.h"
#include "udf_config.h"

namespace fs = std::filesystem;
using namespace plugin::udf;
using jogasaki::location_prefix;

using blocked_stem_map = std::map<std::string, std::set<std::string>>;

namespace {

[[nodiscard]] bool validate_directory(fs::path const& path, std::vector<load_result>& results) {
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

[[nodiscard]] blocked_stem_map build_blocked_stem_map(
    jogasaki::udf::descriptor::message_diagnostics const& diagnostics) {
    blocked_stem_map result{};

    for (auto const& [message_name, diag] : diagnostics) {
        for (auto const& proto : diag.defining_protos) {
            auto stem = fs::path(proto).stem().string();
            result[stem].insert(message_name);
        }

        for (auto const& proto : diag.referring_protos) {
            auto stem = fs::path(proto).stem().string();
            result[stem].insert(message_name);
        }
    }
    return result;
}

void log_message_duplicate_diagnostics(
    jogasaki::udf::descriptor::message_diagnostics const& diagnostics) {
    for (auto const& [message_name, diag] : diagnostics) {
        std::ostringstream oss;
        oss << R"({"event":"udf_message_duplicate","message":")" << message_name
            << R"(","defined_in":[)";

        bool first = true;
        for (auto const& proto : diag.defining_protos) {
            if (!first) { oss << ","; }
            first = false;
            oss << "\"" << proto << "\"";
        }

        oss << R"(],"referred_by":[)";
        first = true;
        for (auto const& proto : diag.referring_protos) {
            if (!first) { oss << ","; }
            first = false;
            oss << "\"" << proto << "\"";
        }
        oss << "]}";

        LOG_LP(WARNING) << jogasaki::udf::log::prefix << oss.str();
    }
}

[[nodiscard]] std::string normalize_plugin_stem(std::string stem) {
    constexpr std::string_view prefix = "lib";
    if (stem.size() >= prefix.size() && stem.compare(0, prefix.size(), prefix) == 0) {
        return stem.substr(prefix.size());
    }
    return stem;
}

void log_blocked_plugin(fs::path const& so_path, std::set<std::string> const& conflicting_messages,
    jogasaki::udf::descriptor::message_diagnostics const& diagnostics) {
    std::ostringstream oss;
    oss << R"({"event":"udf_plugin_skip","plugin":")" << so_path.filename().string()
        << R"(","reason":"message_definition_duplicated","messages":[)";

    bool first_message = true;
    for (auto const& message_name : conflicting_messages) {
        auto it = diagnostics.find(message_name);
        if (it == diagnostics.end()) { continue; }

        auto const& diag = it->second;

        if (!first_message) { oss << ","; }
        first_message = false;

        oss << R"({"message":")" << message_name << R"(","defined_in":[)";

        bool first = true;
        for (auto const& proto : diag.defining_protos) {
            if (!first) { oss << ","; }
            first = false;
            oss << "\"" << proto << "\"";
        }

        oss << R"(],"referred_by":[)";
        first = true;
        for (auto const& proto : diag.referring_protos) {
            if (!first) { oss << ","; }
            first = false;
            oss << "\"" << proto << "\"";
        }

        oss << "]}";
    }

    oss << "]}";

    LOG_LP(WARNING) << jogasaki::udf::log::prefix << oss.str();
}

[[nodiscard]] bool validate_and_prepare_descriptors(std::vector<fs::path> const& desc_files,
    std::string_view dir_path, std::vector<load_result>& results, blocked_stem_map& blocked_stems,
    jogasaki::udf::descriptor::message_diagnostics& message_duplicates) {
    if (desc_files.empty()) {
        blocked_stems.clear();
        message_duplicates.clear();
        LOG_LP(WARNING) << jogasaki::udf::log::prefix << "No descriptor files found in \""
                        << dir_path << "\", SKIPPED RPC/message duplication checks";
        return true;
    }
    auto analysis = jogasaki::udf::descriptor::analyze_descriptors(desc_files);
    if (analysis.has_error()) {
        for (auto const& error : analysis.errors()) {
            switch (error.status) {
                case plugin::udf::descriptor_read_status::ok: break;
                case plugin::udf::descriptor_read_status::descriptor_open_failed:
                    results.emplace_back(plugin::udf::load_status::descriptor_open_failed,
                        error.path.string(), "Failed to open descriptor file, SKIPPED ALL UDFs");
                    break;
                case plugin::udf::descriptor_read_status::descriptor_parse_failed:
                    results.emplace_back(plugin::udf::load_status::descriptor_parse_failed,
                        error.path.string(), "Failed to parse descriptor file, SKIPPED ALL UDFs");
                    break;
            }
        }
        return false;
    }

    auto const rpc_status = jogasaki::udf::descriptor::validation::validate_rpc_method_duplicates(
        analysis.rpc_methods());
    if (rpc_status != plugin::udf::rpc_duplicate_check_status::ok) {
        results.emplace_back(plugin::udf::load_status::rpc_name_duplicated, std::string(dir_path),
            "RPC name duplicated, SKIPPED ALL UDFs");
        return false;
    }

    LOG_LP(INFO) << jogasaki::udf::log::prefix << "No RPC name duplication detected in descriptors";

    message_duplicates =
        jogasaki::udf::descriptor::validation::filter_message_definition_duplicates(
            analysis.message_info());
    blocked_stems = build_blocked_stem_map(message_duplicates);

    if (message_duplicates.empty()) {
        LOG_LP(INFO) << jogasaki::udf::log::prefix
                     << "No message definition duplication detected in descriptors";
    } else {
        log_message_duplicate_diagnostics(message_duplicates);
    }
    return true;
}

} // namespace

[[nodiscard]] std::string const& client_info::default_endpoint() const noexcept {
    return default_endpoint_;
}

[[nodiscard]] bool client_info::default_secure() const noexcept { return default_secure_; }

void client_info::set_default_endpoint(std::string endpoint) {
    default_endpoint_ = std::move(endpoint);
}

void client_info::set_default_secure(bool secure) { default_secure_ = secure; }

std::optional<udf_config> udf_loader::parse_ini(
    fs::path const& ini_path, std::vector<load_result>& results) {
    try {
        boost::property_tree::ptree pt;
        boost::property_tree::ini_parser::read_ini(ini_path.string(), pt);

        auto enabled_opt = pt.get_optional<std::string>("udf.enabled");
        if (!enabled_opt) {
            results.emplace_back(
                load_status::ini_invalid, ini_path.string(), "Missing required field: udf.enabled");
            return std::nullopt;
        }

        std::string enabled_str = *enabled_opt;
        std::transform(enabled_str.begin(), enabled_str.end(), enabled_str.begin(), ::tolower);
        bool enabled = true;
        if (enabled_str == "true") {
            enabled = true;
        } else if (enabled_str == "false") {
            enabled = false;
        } else {
            results.emplace_back(load_status::ini_invalid, ini_path.string(),
                "Invalid value for udf.enabled (must be true or false)");
            return std::nullopt;
        }

        std::string endpoint = std::string(jogasaki::global::config_pool()->endpoint());
        if (auto opt = pt.get_optional<std::string>("udf.endpoint")) { endpoint = *opt; }

        std::string transport = "stream";
        if (auto opt = pt.get_optional<std::string>("udf.transport")) {
            transport = *opt;
            if (transport.empty()) { transport = "stream"; }
        }

        bool secure = jogasaki::global::config_pool()->secure();
        if (auto opt = pt.get_optional<std::string>("udf.secure")) {
            std::string val = *opt;
            std::transform(val.begin(), val.end(), val.begin(), ::tolower);
            if (val == "false") {
                secure = false;
            } else if (val == "true") {
                secure = true;
            } else {
                results.emplace_back(load_status::ini_invalid, ini_path.string(),
                    "Invalid value for udf.secure (must be true or false)");
                return std::nullopt;
            }
        }

        return udf_config(enabled, std::move(endpoint), std::move(transport), secure);

    } catch (std::exception const& e) {
        results.emplace_back(load_status::ini_invalid, ini_path.string(), e.what());
        return std::nullopt;
    }
}

std::vector<load_result> udf_loader::load(std::string_view dir_path) {
    fs::path path(dir_path);
    std::vector<load_result> results{};
    std::vector<std::filesystem::path> ini_files{};
    std::vector<std::filesystem::path> desc_files{};
    blocked_stem_map blocked_stems{};
    jogasaki::udf::descriptor::message_diagnostics message_duplicates{};

    if (dir_path.empty()) {
        return {load_result(load_status::path_is_empty, "", "Directory path is empty")};
    }
    if (!validate_directory(path, results)) { return results; }

    ini_files = collect_ini_files(path);
    if (ini_files.empty()) {
        results.emplace_back(
            load_status::no_ini_files, std::string(dir_path), "No .ini files found (UDF disabled)");
        return results;
    }

    desc_files = collect_desc_pb_files(path);
    if (!validate_and_prepare_descriptors(
            desc_files, dir_path, results, blocked_stems, message_duplicates)) {
        return results;
    }

    for (auto const& ini_path : ini_files) {
        load_one_plugin(ini_path, blocked_stems, message_duplicates, results);
    }
    return results;
}

void udf_loader::unload_all() {
    plugins_.clear();
    for (auto* h : handles_) {
        if (h) { dlclose(h); }
    }
    handles_.clear();
}

load_result udf_loader::create_api_from_handle(
    void* handle, std::string const& full_path, std::shared_ptr<const udf_config> cfg) {
    if (!handle) { return {load_status::dlopen_failed, "", "Invalid handle (nullptr)"}; }

    using create_api_func = plugin_api* (*)();
    using create_factory_func = generic_client_factory* (*)(char const*);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* api_func = reinterpret_cast<create_api_func>(dlsym(handle, "create_plugin_api"));
    if (!api_func) {
        return {load_status::api_symbol_missing, full_path, "Symbol 'create_plugin_api' not found"};
    }

    auto api_uptr = std::unique_ptr<plugin_api>(api_func());
    if (!api_uptr) {
        return {load_status::api_init_failed, full_path, "Failed to initialize plugin API"};
    }
    std::shared_ptr<plugin_api> api_sptr = std::move(api_uptr);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* factory_func = reinterpret_cast<create_factory_func>(
        dlsym(handle, "tsurugi_create_generic_client_factory"));
    if (!factory_func) {
        return {load_status::factory_symbol_missing, full_path,
            "Symbol 'tsurugi_create_generic_client_factory' not found"};
    }

    auto factory_ptr = std::unique_ptr<generic_client_factory>(factory_func("Greeter"));
    if (!factory_ptr) {
        return {load_status::factory_symbol_missing, full_path,
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
    if (!raw_client) {
        return {load_status::factory_creation_failed, full_path,
            "Failed to create generic client from factory"};
    }

    plugins_.emplace_back(
        std::move(api_sptr), std::shared_ptr<generic_client>(raw_client), std::move(cfg));
    return {load_status::ok, full_path, "Loaded successfully"};
}

void udf_loader::load_one_plugin(fs::path const& ini_path, blocked_stem_map const& blocked_stems,
    jogasaki::udf::descriptor::message_diagnostics const& message_duplicates,
    std::vector<load_result>& results) {
    auto raw_stem = ini_path.stem().string();
    auto stem = normalize_plugin_stem(raw_stem);

    auto so_path = ini_path;
    so_path.replace_extension(".so");
    if (!fs::exists(so_path)) { return; }
    auto udf_config_value = parse_ini(ini_path, results);
    if (!udf_config_value) { return; }

    if (!udf_config_value->enabled()) {
        results.emplace_back(
            load_status::udf_disabled, ini_path.string(), "UDF disabled in configuration");
        return;
    }
    if (auto it = blocked_stems.find(stem); it != blocked_stems.end()) {
        log_blocked_plugin(so_path, it->second, message_duplicates);
        results.emplace_back(load_status::message_name_duplicated, so_path.string(),
            "Skipped loading due to duplicate message definition");
        return;
    }

    VLOG(jogasaki::log_trace) << jogasaki::udf::log::prefix
                              << "UDF library found: " << so_path.string()
                              << " (config: " << ini_path.string() << ")";

    std::string full_path = so_path.string();
    dlerror();
    void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        const char* err = dlerror();
        results.emplace_back(
            load_status::dlopen_failed, full_path, err ? err : "dlopen failed with unknown error");
        return;
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

std::vector<plugin_entry>& udf_loader::get_plugins() noexcept { return plugins_; }

udf_loader::~udf_loader() { unload_all(); }
