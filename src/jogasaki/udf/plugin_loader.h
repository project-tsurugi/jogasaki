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
#include <string_view>
#include <tuple>
#include <vector>

#include "error_info.h"
#include "generic_client.h"
#include "plugin_api.h"
#include "udf_config.h"

namespace plugin::udf {

using plugin_entry = std::tuple<std::shared_ptr<plugin::udf::plugin_api>,
    std::shared_ptr<plugin::udf::generic_client>, std::shared_ptr<const plugin::udf::udf_config>>;
class plugin_loader {
public:

    plugin_loader() = default;
    plugin_loader(plugin_loader const&) = delete;
    plugin_loader& operator=(plugin_loader const&) = delete;
    plugin_loader(plugin_loader&&) = delete;
    plugin_loader& operator=(plugin_loader&&) = delete;
    virtual ~plugin_loader() = default;
    [[nodiscard]] virtual std::vector<load_result> load(std::string_view dir_path) = 0;
    virtual void unload_all() = 0;
    [[nodiscard]] virtual std::vector<plugin_entry>&
    get_plugins() noexcept = 0;
};
}  // namespace plugin::udf
