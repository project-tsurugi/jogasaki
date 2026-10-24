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
#include <memory>
#include <string>
#include <vector>
#include <grpcpp/channel.h>

#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/dist/remote_catalog_manager.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

/**
 * @brief development fetcher that serves catalogs of an already loaded snapshot.
 * @details Used for catalog_source=fixture so that fixture metadata passes through
 * remote_catalog_manager::create() like a fetched catalog. It performs no RPC and is not the GetRemoteCatalog client.
 * A catalog is served for a reference only if its name matches and all of its tables are placed on the server.
 */
class snapshot_remote_catalog_fetcher : public remote_catalog_fetcher {
public:
    explicit snapshot_remote_catalog_fetcher(std::shared_ptr<remote_catalog_snapshot const> source) noexcept;

    [[nodiscard]] status fetch(
        remote_catalog_reference const& reference,
        std::shared_ptr<grpc::Channel> const& channel,
        std::chrono::system_clock::time_point deadline,
        remote_catalog_information& result,
        std::string& diagnostic) override;

    /**
     * @brief references of the catalogs whose tables are all placed on one server, in snapshot order.
     */
    [[nodiscard]] std::vector<remote_catalog_reference> references() const;

private:
    std::shared_ptr<remote_catalog_snapshot const> source_{};
};

}  // namespace jogasaki::dist
