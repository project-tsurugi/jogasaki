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
#include "remote_server_configuration_validation.h"

#include <mutex>
#include <array>
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

std::array<char, 129> channel_log_name(std::string_view input) noexcept {
    std::array<char, 129> result{};
    std::size_t position{};
    for (unsigned char ch : input.substr(0, result.size() - 1)) {
        result[position++] = ch >= 0x21 && ch <= 0x7e ? static_cast<char>(ch) : '_';
    }
    return result;
}

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
            LOG_LP(ERROR) << "invalid remote server configuration: empty name or URI, or duplicate name";
            return status::err_invalid_argument;
        }
        if (!valid_remote_server_credentials(configuration)) {
            LOG_LP(ERROR) << "invalid remote server credentials: name=" << configuration.name;
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
                          << " uri_scheme=" << remote_server_uri_scheme(configuration.uri)
                          << " secure=" << std::boolalpha << configuration.secure;
            return status::err_unknown;
        }
        auto channel = grpc::CreateCustomChannel(configuration.uri, credentials, arguments);
        if (!channel) {
            LOG_LP(ERROR) << "failed to create remote server channel: name=" << configuration.name
                          << " uri_scheme=" << remote_server_uri_scheme(configuration.uri)
                          << " secure=" << std::boolalpha << configuration.secure;
            return status::err_unknown;
        }
        channels.emplace(configuration.name, std::move(channel));
    }

    channels_ = std::move(channels);
    statistics_->channels.store(channels_.size(), std::memory_order_relaxed);
    state_ = state::available;
    return status::ok;
}

std::shared_ptr<grpc::Channel>
remote_server_channel_manager::find_channel(std::string_view remote_server_name) const noexcept {
    statistics_->acquisitions.fetch_add(1, std::memory_order_relaxed);
    std::shared_lock lock{mutex_};
    if (state_ != state::available) {
        statistics_->failures.fetch_add(1, std::memory_order_relaxed);
        auto const* current = state_ == state::uninitialized ? "uninitialized" :
                              state_ == state::stopping ? "stopping" : "stopped";
        lock.unlock();
        auto name = channel_log_name(remote_server_name);
        LOG_LP(INFO) << "distributed query channel event:acquire_failed server:" << name.data()
                     << " state:" << current << " reason:manager_unavailable";
        return {};
    }
    if (auto it = channels_.find(std::string{remote_server_name}); it != channels_.end()) {
        return it->second;
    }
    statistics_->failures.fetch_add(1, std::memory_order_relaxed);
    lock.unlock();
    auto name = channel_log_name(remote_server_name);
    LOG_LP(INFO) << "distributed query channel event:acquire_failed server:" << name.data()
                 << " state:available reason:unregistered_server";
    return {};
}

void remote_server_channel_manager::stop() noexcept {
    std::unique_lock lock{mutex_};
    if (state_ == state::stopped) {
        return;
    }
    state_ = state::stopping;
    channels_.clear();
    statistics_->channels.store(0, std::memory_order_relaxed);
    state_ = state::stopped;
}

void remote_server_channel_manager::register_metrics(tateyama::metrics::metrics_store& store) const {
    auto stats = statistics_;
    auto add = [&](char const* key, auto reader) {
        // A new database manager replaces this component's previous generation of metrics.
        store.unregister_element(key);
        store.register_item(tateyama::metrics::metrics_metadata{key, key, {}, {}}, std::move(reader));
    };
    add("distributed_query_channels", [stats] { return static_cast<double>(stats->channels.load(std::memory_order_relaxed)); });
    add("distributed_query_channel_acquisitions_total", [stats] { return static_cast<double>(stats->acquisitions.load(std::memory_order_relaxed)); });
    add("distributed_query_channel_failures_total", [stats] { return static_cast<double>(stats->failures.load(std::memory_order_relaxed)); });
}

} // namespace jogasaki::dist
