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
#include "generic_client_factory.h"

#include "generic_record_impl.h"
#include <dlfcn.h>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
namespace fs = std::filesystem;
using namespace plugin::udf;
void udf_loader::load(std::string_view dir_path) {
    fs::path path(dir_path);

    std::vector<fs::path> files_to_load;
    if (fs::is_regular_file(path) && path.extension() == ".so") {
        files_to_load.push_back(path);
    } else if (fs::is_directory(path)) {
        for (const auto& entry : fs::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".so") {
                files_to_load.push_back(entry.path());
            }
        }
    } else {
        std::cerr << "Plugin path is not valid or not a .so file: " << path << std::endl;
        return;
    }

    for (const auto& file : files_to_load) {
        std::string full_path = file.string();
        dlerror();
        void* handle    = dlopen(full_path.c_str(), RTLD_NOW | RTLD_GLOBAL);
        const char* err = dlerror();
        if (!handle || err) {
            std::cerr << "Failed to load " << full_path << ": " << (err ? err : "unknown error")
                      << std::endl;
            continue;
        }
        handles_.emplace_back(handle);
        create_api_from_handle(handle);
    }
}

void udf_loader::unload_all() {
    plugins_.clear();
    for (void* handle : handles_) {
        if (handle) dlclose(handle);
    }
    handles_.clear();
}
void udf_loader::create_api_from_handle(void* handle) {
    if (!handle) return;

    using create_api_func     = plugin_api* (*)();
    using create_factory_func = generic_client_factory* (*)(const char*);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* api_func = reinterpret_cast<create_api_func>(dlsym(handle, "create_plugin_api"));
    if (!api_func) {
        std::cerr << "Failed to find symbol create_plugin_api\n";
        return;
    }

    auto api_ptr = std::unique_ptr<plugin_api>(api_func());
    if (!api_ptr) {
        std::cerr << "create_plugin_api returned nullptr\n";
        return;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* factory_func = reinterpret_cast<create_factory_func>(
        dlsym(handle, "tsurugi_create_generic_client_factory"));
    if (!factory_func) {
        std::cerr << "Failed to find symbol tsurugi_create_generic_client_factory\n";
        return;
    }

    auto factory_ptr = std::unique_ptr<generic_client_factory>(factory_func("Greeter"));
    if (!factory_ptr) {
        std::cerr << "tsurugi_create_generic_client_factory returned nullptr\n";
        return;
    }

    plugins_.emplace_back(api_ptr.release(), factory_ptr.release());
}

const std::vector<std::tuple<plugin_api*, generic_client_factory*>>&
udf_loader::get_plugins() const noexcept {
    return plugins_;
}
udf_loader::~udf_loader() { unload_all(); }
