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




#include <string>

#include <yugawara/schema/catalog.h>
#include <tateyama/proto/distributed_query/remote_catalog.pb.h>

#include <jogasaki/status.h>

namespace jogasaki::dist {

/**
 * @brief remote-side L3-02 Facade that encodes a local Yugawara catalog into a complete L3-07 RemoteCatalog.
 * @details The only entry point for the GetRemoteCatalog handler. describe_remote_catalog() builds the encoding
 * plan (identity-based enumeration, ownership, order and transfer indices); the message encoding then follows the
 * plan without re-enumerating providers or renumbering. Fragment splitting belongs to the RPC handler (Tateyama).
 */
class remote_catalog_encoder {
public:
    /**
     * @return status::ok, status::err_unsupported for a declaration or data type outside remote execution
     * (BIT, INTERVAL, array, record, row_reference, declared, extension, or an unknown feature),
     * or status::err_unknown for any other failure; destination is unchanged on failure
     */
    [[nodiscard]] status encode(
        ::yugawara::schema::catalog const& source,
        ::tateyama::grpc::proto::RemoteCatalog& destination) const;

    /**
     * @brief encode() that also reports the reason of a failure.
     */
    [[nodiscard]] status encode(
        ::yugawara::schema::catalog const& source,
        ::tateyama::grpc::proto::RemoteCatalog& destination,
        std::string& diagnostic) const;
};

}  // namespace jogasaki::dist
