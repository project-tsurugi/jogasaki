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
#include "rpc_remote_catalog_fetcher.h"

#include <atomic>
#include <cstdint>
#include <exception>
#include <utility>
#include <glog/logging.h>
#include <grpcpp/client_context.h>

#include <tateyama/proto/distributed_query/remote_catalog.grpc.pb.h>

#include <jogasaki/dist/remote_catalog_decoder.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki::dist {

namespace {

namespace wire = ::tateyama::grpc::proto;

} // namespace

[[nodiscard]] static status catalog_status(grpc::StatusCode code) noexcept {
    switch (code) {
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            return status::err_time_out;
        case grpc::StatusCode::UNAVAILABLE:
            return status::err_io_error;
        case grpc::StatusCode::CANCELLED:
            return status::err_io_error;
        case grpc::StatusCode::UNIMPLEMENTED:
            return status::err_unsupported;
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
            return status::err_resource_limit_reached;
        case grpc::StatusCode::INVALID_ARGUMENT:
            return status::err_invalid_argument;
        case grpc::StatusCode::FAILED_PRECONDITION:
            return status::err_invalid_state;
        case grpc::StatusCode::DATA_LOSS:
            return status::err_data_corruption;
        default:
            return status::err_unknown;
    }
}

[[nodiscard]] static std::string next_request_id(remote_catalog_reference const& reference) {
    static std::atomic_uint64_t sequence{};
    return "catalog-" + reference.catalog_name + "-" + std::to_string(++sequence);
}

status rpc_remote_catalog_fetcher::fetch(remote_catalog_reference const& reference,
                                         std::shared_ptr<grpc::Channel> const& channel,
                                         std::chrono::system_clock::time_point deadline,
                                         remote_catalog_information& result, std::string& diagnostic) {
    if (!channel) {
        diagnostic = "no channel for remote server " + reference.remote_server_name;
        return status::err_invalid_argument;
    }
    auto started = std::chrono::steady_clock::now();
    wire::RemoteCatalogRequest request{};
    auto* metadata = request.mutable_metadata();
    metadata->mutable_protocol_version()->set_major(1);
    metadata->mutable_protocol_version()->set_minor(0);
    metadata->set_request_id(next_request_id(reference));

    remote_catalog_fragment_assembler assembler{};
    status wire_status = status::ok;
    std::string wire_error{};
    grpc::Status final{};
    try {
        grpc::ClientContext context{};
        context.set_deadline(deadline);
        auto stub = wire::DistributedQueryService::NewStub(channel);
        auto reader = stub->GetRemoteCatalog(&context, request);
        wire::RemoteCatalogResponse response{};
        while (reader->Read(&response)) {
            if (wire_status != status::ok) {
                continue; // drain after cancellation
            }
            if (auto rc = assembler.accept(response, wire_error); rc != status::ok) {
                wire_status = rc;
                context.TryCancel();
            }
        }
        final = reader->Finish();
    } catch (std::exception const& e) {
        assembler.reset();
        diagnostic = std::string{"GetRemoteCatalog failed: "} + e.what();
        return status::err_unknown;
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    auto fragments = assembler.fragments();
    auto bytes = assembler.bytes();
    status rc = status::ok;
    if (wire_status != status::ok) {
        rc = wire_status;
        diagnostic = "GetRemoteCatalog stream violated the wire contract: " + wire_error;
    } else if (!final.ok()) {
        assembler.reset();
        rc = catalog_status(final.error_code());
        diagnostic = "GetRemoteCatalog failed with gRPC status " + std::to_string(final.error_code()) + ": " +
                     final.error_message();
    } else {
        remote_catalog_information decoded{};
        rc = assembler.finish(decoded, diagnostic);
        if (rc == status::ok) {
            result = std::move(decoded);
        }
    }
    LOG_LP(INFO) << "GetRemoteCatalog request_id:" << metadata->request_id() << " catalog:" << reference.catalog_name
                 << " server:" << reference.remote_server_name << " status:" << rc
                 << " grpc_status:" << final.error_code() << " fragments:" << (rc == status::ok ? fragments : 0)
                 << " response_bytes:" << (rc == status::ok ? bytes : 0) << " request_bytes:" << request.ByteSizeLong()
                 << " elapsed_ms:" << elapsed.count() << (rc == status::ok ? "" : " reason:" + diagnostic);
    return rc;
}

} // namespace jogasaki::dist
