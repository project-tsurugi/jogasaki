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


#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <grpcpp/channel.h>

#include <jogasaki/dist/remote_server_configuration.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

class remote_server_channel_manager {
public:
    remote_server_channel_manager() = default;
    ~remote_server_channel_manager() = default;

    remote_server_channel_manager(remote_server_channel_manager const&) = delete;
    remote_server_channel_manager& operator=(remote_server_channel_manager const&) = delete;
    remote_server_channel_manager(remote_server_channel_manager&&) = delete;
    remote_server_channel_manager& operator=(remote_server_channel_manager&&) = delete;

    [[nodiscard]] status start(std::vector<remote_server_configuration> const& configurations);

    [[nodiscard]] std::shared_ptr<grpc::Channel> find_channel(
        std::string_view remote_server_name) const noexcept;

    void stop() noexcept;

private:
    enum class state {
        uninitialized,
        available,
        stopping,
        stopped,
    };

    mutable std::shared_mutex mutex_{};
    state state_{state::uninitialized};
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels_{};
};

} // namespace jogasaki::dist
