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
#pragma once
#include <jogasaki/dist/remote_catalog_provider.h>

namespace jogasaki::dist {
/** @brief Load development fixture catalogs without publishing partial metadata. */
status load_fixture_catalogs(std::string const& path, std::vector<remote_server_configuration> const& servers,
                             std::shared_ptr<remote_catalog_snapshot const>& result, std::string& diagnostic);
} // namespace jogasaki::dist
