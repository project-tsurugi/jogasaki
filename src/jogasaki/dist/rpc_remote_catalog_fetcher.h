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
#include <grpcpp/channel.h>

#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/dist/remote_catalog_manager.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

/**
 * @brief GetRemoteCatalog client (L3-07) used by remote_catalog_manager for catalog_source=rpc.
 * @details Sends one request over the given channel with the manager's deadline, validates and reassembles the
 * stream, and decodes the complete catalog. The result is set only if the stream ended with OK and every
 * fragment and the decoded catalog were valid; otherwise all received fragments are discarded.
 */
class rpc_remote_catalog_fetcher : public remote_catalog_fetcher {
public:
    /**
     * @return status::ok, or the failure mapped from the gRPC status
     * (err_time_out for DEADLINE_EXCEEDED, err_io_error for UNAVAILABLE or CANCELLED, err_unsupported for
     * UNIMPLEMENTED, err_resource_limit_reached for RESOURCE_EXHAUSTED, err_invalid_argument for INVALID_ARGUMENT,
     * err_invalid_state for FAILED_PRECONDITION, err_data_corruption for DATA_LOSS, err_unknown otherwise),
     * or the status of a wire contract violation detected here
     */
    [[nodiscard]] status fetch(
        remote_catalog_reference const& reference,
        std::shared_ptr<grpc::Channel> const& channel,
        std::chrono::system_clock::time_point deadline,
        remote_catalog_information& result,
        std::string& diagnostic) override;
};

}  // namespace jogasaki::dist
