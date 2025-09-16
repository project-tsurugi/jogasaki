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
#pragma once
#include "error_info.h"
#include "generic_client_factory.h"
#include "plugin_api.h"
#include "plugin_loader.h"
#include <string_view>
#include <tuple>
#include <vector>
namespace plugin::udf {
/**
 * @brief Loader for dynamically loading and unloading User Defined Function (UDF) plugins.
 *
 * This class is responsible for discovering, loading, and managing plugin shared libraries
 * (.so files) that implement the required UDF interfaces. It uses `dlopen(3)` and `dlsym(3)`
 * to dynamically load symbols and create plugin API instances.
 *
 * Key responsibilities:
 * - Load plugins from a single shared object file or all .so files in a directory.
 * - Resolve required symbols (`create_plugin_api`, `tsurugi_create_generic_client_factory`).
 * - Store loaded plugin handles for later unloading.
 * - Provide access to the loaded `plugin_api` and `generic_client_factory` instances.
 *
 * Notes:
 * - Only files with `.so` extension are considered.
 * - `plugin_api` and `generic_client_factory` lifetime management is delegated to the caller.
 * - Uses `RTLD_NOW | RTLD_GLOBAL`:
 *   - **RTLD_NOW**: Resolve all undefined symbols immediately at load time (fail early if missing).
 *   - **RTLD_GLOBAL**: Make the loaded symbols available for symbol resolution in subsequently
 * loaded libraries.
 *
 * @see plugin_loader
 */
class udf_loader : public plugin_loader {
  public:
    udf_loader()                             = default;
    udf_loader(const udf_loader&)            = delete;
    udf_loader& operator=(const udf_loader&) = delete;
    udf_loader(udf_loader&&)                 = delete;
    udf_loader& operator=(udf_loader&&)      = delete;
    ~udf_loader() override;
    /**
     * @brief Loads UDF plugins from the specified path.
     *
     * If the path is a directory, all `.so` files in it will be loaded.
     * If the path is a single file, it will be loaded only if it has a `.so` extension.
     *
     * @param dir_path Path to a plugin file or a directory containing plugins.
     */
    [[nodiscard]] load_result load(std::string_view dir_path) override;
    /**
     * @brief Unloads all currently loaded plugins.
     *
     * Calls `dlclose()` on each loaded handle and clears the internal handle list.
     * Safe to call multiple times.
     */
    void unload_all() override;
    /**
     * @brief Retrieves the list of loaded plugin API/factory pairs.
     *
     * @return Vector of tuples containing (`plugin_api*`, `generic_client_factory*`).
     *         The pointers remain valid until `unload_all()` is called.
     */
    [[nodiscard]] const std::vector<std::tuple<plugin_api*, generic_client_factory*>>&
    get_plugins() const noexcept override;

  private:
    std::vector<void*> handles_;
    /** List of raw `dlopen()` handles for loaded plugins. */
    [[nodiscard]] load_result create_api_from_handle(void* handle);
    /** List of loaded plugin API/factory pairs. */
    std::vector<std::tuple<plugin_api*, generic_client_factory*>> plugins_;
};
} // namespace plugin::udf
