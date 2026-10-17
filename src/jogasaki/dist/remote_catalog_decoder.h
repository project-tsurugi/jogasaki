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




#include <cstddef>
#include <cstdint>
#include <string>

#include <tateyama/proto/distributed_query/remote_catalog.pb.h>

#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

/// @brief L3-07 serialized size limit of each GetRemoteCatalog response.
constexpr std::size_t remote_catalog_max_message_size = std::size_t{64} * 1024U * 1024U;

/**
 * @brief coordinator-side decoder of a complete L3-07 RemoteCatalog into remote_catalog_information.
 * @details Performs wire validation only: RemoteDataType payloads, enum values, required messages and index ranges.
 * Optional indices stay absent rather than becoming 0. Ownership, names and semantics are validated afterwards by
 * remote_catalog_manager::create() (L3-02).
 */
class remote_catalog_decoder {
public:
    /**
     * @return status::ok,
     * status::err_unsupported for an unknown oneof case or enum value, or a remote execution excluded type (BIT, INTERVAL),
     * status::err_data_corruption for a missing oneof value or required message, an unspecified or duplicate enum
     * value, an invalid type parameter, or an index out of range;
     * destination is unchanged on failure
     */
    [[nodiscard]] status decode(
        ::tateyama::grpc::proto::RemoteCatalog const& source,
        remote_catalog_information& destination,
        std::string& diagnostic) const;
};

/**
 * @brief reassembles GetRemoteCatalog responses into one complete catalog.
 * @details Validates sequence numbers, fragment presence and size. Any failure discards everything received so far,
 * and the assembler then rejects further input until reset().
 */
class remote_catalog_fragment_assembler {
public:
    /**
     * @param response the next response; its elements are moved out
     * @return status::ok,
     * status::err_data_corruption for a sequence gap or an absent or empty fragment,
     * status::err_resource_limit_reached for a response over the message limit,
     * status::err_invalid_state after a failure
     */
    [[nodiscard]] status accept(::tateyama::grpc::proto::RemoteCatalogResponse& response, std::string& diagnostic);

    /**
     * @brief decodes the catalog received so far; call only after the stream ended with OK.
     * @details The assembler is reset afterwards in any case.
     */
    [[nodiscard]] status finish(remote_catalog_information& destination, std::string& diagnostic);

    void reset() noexcept;

    [[nodiscard]] std::uint64_t fragments() const noexcept;
    [[nodiscard]] std::size_t bytes() const noexcept;

private:
    ::tateyama::grpc::proto::RemoteCatalog catalog_{};
    std::uint64_t next_{};
    std::size_t bytes_{};
    bool failed_{};

    status fail(status rc, std::string message, std::string& diagnostic);
};

}  // namespace jogasaki::dist
