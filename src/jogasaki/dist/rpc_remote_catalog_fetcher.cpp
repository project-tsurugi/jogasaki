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
#include "remote_rpc_error.h"

#include <atomic>
#include <cstdint>
#include <exception>
#include <utility>
#include <glog/logging.h>
#include <grpcpp/client_context.h>

#include <tateyama/proto/distributed_query/remote_catalog.grpc.pb.h>

#include <jogasaki/dist/remote_catalog_decoder.h>
#include <jogasaki/logging.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki::dist {

namespace {

namespace wire = ::tateyama::grpc::proto;

} // namespace

[[nodiscard]] static std::string next_request_id(remote_catalog_reference const& reference) {
    static std::atomic_uint64_t sequence{};
    return "catalog-" + reference.catalog_name + "-" + std::to_string(++sequence);
}

status rpc_remote_catalog_fetcher::fetch(remote_catalog_reference const& reference,
                                         std::shared_ptr<grpc::Channel> const& channel,
                                         std::chrono::system_clock::time_point deadline,
                                         remote_catalog_information& result, std::string& diagnostic,
                                         std::shared_ptr<error::error_info>* failure) {
    if (failure) failure->reset();
    if (!channel) {
        diagnostic = "[distributed-query] remote server channel not found";
        if (failure) {
            *failure = create_error_info(error_code::sql_execution_exception, diagnostic, status::err_not_found);
        }
        LOG_LP(INFO) << "GetRemoteCatalog server:" << reference.remote_server_name
                     << " internal_code:REMOTE_CHANNEL_NOT_FOUND";
        return status::err_not_found;
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
        LOG_LP(INFO) << "GetRemoteCatalog server:" << reference.remote_server_name
                     << " internal_code:REMOTE_TRANSPORT_EXCEPTION reason:" << e.what();
        diagnostic = "[distributed-query] unexpected remote transport error";
        if (failure) {
            *failure = create_error_info(error_code::internal_exception, diagnostic, status::err_unknown);
        }
        return status::err_unknown;
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    auto fragments = assembler.fragments();
    auto bytes = assembler.bytes();
    status rc = status::ok;
    if (wire_status != status::ok) {
        rc = wire_status == status::err_data_corruption ? status::err_invalid_state : wire_status;
        LOG_LP(INFO) << "GetRemoteCatalog server:" << reference.remote_server_name
                     << " internal_code:INVALID_REMOTE_CATALOG reason:" << wire_error;
        diagnostic = "[distributed-query] invalid remote catalog definition";
    } else if (!final.ok()) {
        assembler.reset();
        auto mapped = classify_remote_rpc_error(final.error_code());
        rc = mapped.result;
        diagnostic = remote_rpc_message("GetRemoteCatalog", mapped);
        if (failure) {
            *failure = create_error_info(mapped.code, diagnostic, rc);
        }
    } else {
        remote_catalog_information decoded{};
        rc = assembler.finish(decoded, diagnostic);
        if (rc == status::err_data_corruption || rc == status::err_unsupported) {
            LOG_LP(INFO) << "GetRemoteCatalog server:" << reference.remote_server_name
                         << " internal_code:" << (rc == status::err_unsupported
                              ? "UNSUPPORTED_REMOTE_CATALOG" : "INVALID_REMOTE_CATALOG")
                         << " reason:" << diagnostic;
            diagnostic = rc == status::err_unsupported ? "[distributed-query] unsupported remote catalog definition"
                                                       : "[distributed-query] invalid remote catalog definition";
            if (rc == status::err_data_corruption) {
                rc = status::err_invalid_state;
            }
        }
        if (rc == status::ok) {
            result = std::move(decoded);
        }
    }
    if (failure && !*failure && rc != status::ok) {
        auto code = rc == status::err_unsupported ? error_code::unsupported_runtime_feature_exception
                    : rc == status::err_invalid_state ? error_code::internal_exception
                                                     : error_code::sql_execution_exception;
        *failure = create_error_info(code, diagnostic, rc);
    }
    LOG_LP(INFO) << "GetRemoteCatalog request_id:" << metadata->request_id() << " catalog:" << reference.catalog_name
                 << " server:" << reference.remote_server_name << " status:" << rc
                 << " grpc_status:" << final.error_code() << " grpc_message:" << final.error_message()
                 << " fragments:" << (rc == status::ok ? fragments : 0)
                 << " response_bytes:" << (rc == status::ok ? bytes : 0) << " request_bytes:" << request.ByteSizeLong()
                 << " elapsed_ms:" << elapsed.count() << (rc == status::ok ? "" : " reason:" + diagnostic);
    return rc;
}

} // namespace jogasaki::dist
