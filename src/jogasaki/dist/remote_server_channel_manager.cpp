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

#include "remote_server_channel_manager.h"

#include <mutex>
#include <unordered_set>
#include <utility>
#include <glog/logging.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki::dist {

namespace {

constexpr int max_message_size = 64 * 1024 * 1024;

} // namespace

[[nodiscard]] static std::shared_ptr<grpc::ChannelCredentials>
create_credentials(remote_server_configuration const& configuration) {
    if (!configuration.secure) {
        return grpc::InsecureChannelCredentials();
    }

    grpc::SslCredentialsOptions options{};
    switch (configuration.root_certificate) {
        case root_certificate_source::none:
        case root_certificate_source::grpc_default_roots:
            break;
        case root_certificate_source::pem:
            options.pem_root_certs = configuration.pem_root_certs;
            break;
    }
    options.pem_private_key.clear();
    options.pem_cert_chain.clear();
    return grpc::SslCredentials(options);
}

status remote_server_channel_manager::start(std::vector<remote_server_configuration> const& configurations) {
    std::unique_lock lock{mutex_};
    if (state_ != state::uninitialized) {
        return status::err_invalid_state;
    }

    std::unordered_set<std::string> names{};
    for (auto const& configuration : configurations) {
        if (configuration.name == "local") {
            LOG_LP(ERROR) << "remote server name 'local' is reserved";
            return status::err_invalid_argument;
        }
        if (configuration.name.empty() || configuration.uri.empty() || !names.emplace(configuration.name).second) {
            return status::err_invalid_argument;
        }
    }
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels{};
    channels.reserve(configurations.size());

    grpc::ChannelArguments arguments{};
    arguments.SetMaxSendMessageSize(max_message_size);
    arguments.SetMaxReceiveMessageSize(max_message_size);

    for (auto const& configuration : configurations) {
        auto credentials = create_credentials(configuration);
        if (!credentials) {
            LOG_LP(ERROR) << "failed to create credentials for remote server channel: name=" << configuration.name
                          << " secure=" << std::boolalpha << configuration.secure;
            return status::err_unknown;
        }
        auto channel = grpc::CreateCustomChannel(configuration.uri, credentials, arguments);
        if (!channel) {
            LOG_LP(ERROR) << "failed to create remote server channel: name=" << configuration.name;
            return status::err_unknown;
        }
        channels.emplace(configuration.name, std::move(channel));
    }

    channels_ = std::move(channels);
    state_ = state::available;
    return status::ok;
}

std::shared_ptr<grpc::Channel>
remote_server_channel_manager::find_channel(std::string_view remote_server_name) const noexcept {
    std::shared_lock lock{mutex_};
    if (state_ != state::available) {
        return {};
    }
    if (auto it = channels_.find(std::string{remote_server_name}); it != channels_.end()) {
        return it->second;
    }
    return {};
}

void remote_server_channel_manager::stop() noexcept {
    std::unique_lock lock{mutex_};
    if (state_ == state::stopped) {
        return;
    }
    state_ = state::stopping;
    channels_.clear();
    state_ = state::stopped;
}

} // namespace jogasaki::dist
