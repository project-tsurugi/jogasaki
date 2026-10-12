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


#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <vector>
#include <grpcpp/channel.h>

#include <yugawara/schema/catalog.h>

#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/remote_function_id_registry.h>
#include <jogasaki/dist/remote_server_channel_manager.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

/**
 * @brief coordinator-side catalog name and the remote server configuration name it is fetched from.
 */
struct remote_catalog_reference {
    std::string catalog_name{};
    std::string remote_server_name{};
};

/**
 * @brief a published remote catalog.
 */
struct remote_catalog_entry {
    remote_catalog_reference reference{};
    std::shared_ptr<::yugawara::schema::catalog const> catalog{};
};

/**
 * @brief obtains one complete remote catalog from a remote server.
 * @details The GetRemoteCatalog client (L3-07 stream, fragment reassembly and wire validation) is supplied
 * through this interface: rpc_remote_catalog_fetcher for catalog_source=rpc, snapshot_remote_catalog_fetcher for
 * the development fixture. Called only from remote_catalog_manager::create(), never retried automatically.
 */
class remote_catalog_fetcher {
public:
    remote_catalog_fetcher() = default;
    remote_catalog_fetcher(remote_catalog_fetcher const&) = default;
    remote_catalog_fetcher& operator=(remote_catalog_fetcher const&) = default;
    remote_catalog_fetcher(remote_catalog_fetcher&&) = default;
    remote_catalog_fetcher& operator=(remote_catalog_fetcher&&) = default;
    virtual ~remote_catalog_fetcher() = default;

    /**
     * @param reference the catalog being created
     * @param channel the channel of reference.remote_server_name, valid only during this call
     * @param deadline the finite deadline of the catalog RPC
     * @param result receives the complete catalog on success
     * @param diagnostic receives the reason on failure
     */
    [[nodiscard]] virtual status fetch(
        remote_catalog_reference const& reference,
        std::shared_ptr<grpc::Channel> const& channel,
        std::chrono::system_clock::time_point deadline,
        remote_catalog_information& result,
        std::string& diagnostic) = 0;
};

/**
 * @brief runtime registry of remote catalogs (L3-02).
 * @details Facade over fetch, resolve and apply. Only create() starts a catalog fetch, and an entry is published
 * only after all stages succeed. Each publication replaces an immutable snapshot, so a snapshot or compiled
 * artifact obtained earlier keeps its catalog, declarations and placements after drop() or stop().
 * Nothing is persisted: start() always begins with no entries.
 */
class remote_catalog_manager {
public:
    using entry_list_type = std::vector<remote_catalog_entry>;

    static constexpr std::chrono::milliseconds default_fetch_timeout{30000};

    /**
     * @param fetcher the GetRemoteCatalog client; create() fails with err_not_implemented if absent
     * @param fetch_timeout the deadline given to each fetch
     * @param function_ids the coordinator-side definition ID registry owned by the caller (the database).
     * The manager only refers to it; create() of a catalog with function declarations fails if it is absent.
     */
    explicit remote_catalog_manager(
        std::shared_ptr<remote_catalog_fetcher> fetcher = {},
        std::chrono::milliseconds fetch_timeout = default_fetch_timeout,
        std::shared_ptr<remote_function_id_registry> const& function_ids = {});

    ~remote_catalog_manager() = default;
    remote_catalog_manager(remote_catalog_manager const&) = delete;
    remote_catalog_manager& operator=(remote_catalog_manager const&) = delete;
    remote_catalog_manager(remote_catalog_manager&&) = delete;
    remote_catalog_manager& operator=(remote_catalog_manager&&) = delete;

    status start();

    status create(remote_catalog_reference const& reference, remote_server_channel_manager const& channel_manager);

    /**
     * @brief create() that also reports the reason of a failure.
     */
    status create(
        remote_catalog_reference const& reference,
        remote_server_channel_manager const& channel_manager,
        std::string& diagnostic);

    status drop(std::string_view catalog_name);

    [[nodiscard]] std::shared_ptr<entry_list_type const> entries() const noexcept;

    [[nodiscard]] std::shared_ptr<::yugawara::schema::catalog const> find_catalog(
        std::string_view catalog_name) const noexcept;

    /**
     * @brief the catalogs and placements used for SQL compilation.
     * @return the current snapshot while available, or empty otherwise
     */
    [[nodiscard]] std::shared_ptr<remote_catalog_snapshot const> snapshot() const noexcept;

    status stop() noexcept;

private:
    enum class state {
        uninitialized,
        initializing,
        available,
        stopped,
    };

    struct published {
        std::shared_ptr<entry_list_type const> entries{};
        // one single-catalog snapshot per entry, in the same order
        std::vector<std::shared_ptr<remote_catalog_snapshot const>> parts{};
        std::shared_ptr<remote_catalog_snapshot const> snapshot{};
    };

    std::shared_ptr<remote_catalog_fetcher> fetcher_{};
    std::chrono::milliseconds fetch_timeout_{};
    std::weak_ptr<remote_function_id_registry> function_ids_{};
    mutable std::mutex mutex_{};
    state state_{state::uninitialized};
    published published_{};
    std::set<std::string, std::less<>> creating_{};

    [[nodiscard]] static published make_published(
        std::vector<remote_catalog_entry> entries,
        std::vector<std::shared_ptr<remote_catalog_snapshot const>> parts);

    [[nodiscard]] status reserve_function_ids(
        remote_catalog_information const& info,
        std::shared_ptr<remote_function_id_lease>& lease,
        std::string& diagnostic) const;
};

/**
 * @brief whether a catalog or server name follows the common identifier rule of the SQL compiler.
 * @details Non-empty and not starting with the reserved prefix "__".
 */
[[nodiscard]] bool valid_remote_catalog_identifier(std::string_view name) noexcept;

}  // namespace jogasaki::dist
