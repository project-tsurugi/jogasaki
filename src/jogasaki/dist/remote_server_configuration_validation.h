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
#include <string_view>
#include <jogasaki/dist/remote_server_configuration.h>
namespace jogasaki::dist {
// INI targets are narrower than the targets accepted by the direct gRPC configuration API.
[[nodiscard]] bool valid_remote_server_uri(std::string_view uri, bool secure) noexcept;
[[nodiscard]] bool valid_remote_root_certificates(std::string_view pem);
[[nodiscard]] bool valid_remote_server_credentials(remote_server_configuration const& configuration);
} // namespace jogasaki::dist
