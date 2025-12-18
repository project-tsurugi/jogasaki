/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <data_relay_grpc/proto/blob_relay/blob_relay_streaming.pb.h>
#include <data_relay_grpc/proto/blob_relay/blob_relay_streaming.grpc.pb.h>
#include <data_relay_grpc/blob_relay/api_version.h>

namespace jogasaki::testing {

/**
 * @brief gRPC client for data-relay-grpc blob relay service
 */
class data_relay_client {
public:
    /**
     * @brief construct data_relay_client
     * @param endpoint gRPC server endpoint (e.g., "localhost:52345")
     */
    explicit data_relay_client(std::string endpoint)
        : endpoint_(std::move(endpoint))
    {}

    /**
     * @brief download blob data via gRPC Get streaming
     * @param session_id session ID
     * @param storage_id storage ID (0: session, 1: datastore)
     * @param blob_id blob object ID
     * @param tag blob reference tag
     * @return blob data as string, or empty string on failure
     */
    std::string get_blob(
        std::uint64_t session_id,
        std::uint64_t storage_id,
        std::uint64_t blob_id,
        std::uint64_t tag
    ) {
        using BlobRelayStreaming = data_relay_grpc::proto::blob_relay::blob_relay_streaming::BlobRelayStreaming;
        using GetStreamingRequest = data_relay_grpc::proto::blob_relay::blob_relay_streaming::GetStreamingRequest;
        using GetStreamingResponse = data_relay_grpc::proto::blob_relay::blob_relay_streaming::GetStreamingResponse;

        auto channel = ::grpc::CreateChannel(endpoint_, ::grpc::InsecureChannelCredentials());
        BlobRelayStreaming::Stub stub(channel);
        ::grpc::ClientContext context{};

        GetStreamingRequest req{};
        req.set_api_version(BLOB_RELAY_API_VERSION);
        req.set_session_id(session_id);
        auto* blob = req.mutable_blob();
        blob->set_storage_id(storage_id);
        blob->set_object_id(blob_id);
        blob->set_tag(tag);

        LOG(INFO) << "[data_relay_client::get_blob] Request: "
                  << "api_version=" << req.api_version()
                  << " session_id=" << req.session_id()
                  << " storage_id=" << blob->storage_id()
                  << " object_id=" << blob->object_id()
                  << " tag=" << blob->tag();

        std::unique_ptr<::grpc::ClientReader<GetStreamingResponse>> reader(
            stub.Get(&context, req));

        std::string blob_data{};
        GetStreamingResponse resp{};

        // Try to read first response
        if (! reader->Read(&resp)) {
            ::grpc::Status status = reader->Finish();
            LOG(ERROR) << "[data_relay_client::get_blob] No response received, status=" << status.error_code()
                       << " message=" << status.error_message();
            if (! status.ok()) {
                takatori::util::throw_exception(std::runtime_error(
                    "get_blob: failed to read response, status=" + std::to_string(status.error_code()) +
                    " message=" + status.error_message()));
            }
            // Empty blob
            return blob_data;
        }

        LOG(INFO) << "[data_relay_client::get_blob] First response payload_case=" << resp.payload_case();
        if (resp.payload_case() == GetStreamingResponse::PayloadCase::kMetadata) {
            LOG(INFO) << "[data_relay_client::get_blob] Metadata received: blob_size=" << resp.metadata().blob_size();
        }

        // Check if first response is metadata (new protocol)
        if (resp.payload_case() == GetStreamingResponse::PayloadCase::kMetadata) {
            // New protocol: metadata first, then chunks
            while (reader->Read(&resp)) {
                if (resp.payload_case() != GetStreamingResponse::PayloadCase::kChunk) {
                    takatori::util::throw_exception(std::runtime_error(
                        "get_blob: unexpected payload type after metadata, expected kChunk but got " +
                        std::to_string(resp.payload_case())));
                }
                blob_data += resp.chunk();
            }
        } else if (resp.payload_case() == GetStreamingResponse::PayloadCase::kChunk) {
            // Old protocol: chunks only (backward compatibility)
            blob_data += resp.chunk();
            while (reader->Read(&resp)) {
                if (resp.payload_case() != GetStreamingResponse::PayloadCase::kChunk) {
                    takatori::util::throw_exception(std::runtime_error(
                        "get_blob: unexpected payload type in old protocol, expected kChunk but got " +
                        std::to_string(resp.payload_case())));
                }
                blob_data += resp.chunk();
            }
        } else {
            takatori::util::throw_exception(std::runtime_error(
                "get_blob: first response has unexpected payload type " +
                std::to_string(resp.payload_case())));
        }

        ::grpc::Status status = reader->Finish();
        if (! status.ok()) {
            LOG(ERROR) << "[data_relay_client::get_blob] RPC failed, status=" << status.error_code()
                       << " message=" << status.error_message();
            takatori::util::throw_exception(std::runtime_error(
                "get_blob: RPC failed, status=" + std::to_string(status.error_code()) +
                " message=" + status.error_message()));
        }
        LOG(INFO) << "[data_relay_client::get_blob] RPC finished successfully, blob_data.size=" << blob_data.size();

        return blob_data;
    }

    /**
     * @brief upload blob data via gRPC Put streaming
     * @param session_id session ID
     * @param data blob data to upload
     * @return pair of (blob_id, storage_id), or (0, 0) on failure
     */
    std::pair<std::uint64_t, std::uint64_t> put_blob(
        std::uint64_t session_id,
        std::string const& data
    ) {
        using BlobRelayStreaming = data_relay_grpc::proto::blob_relay::blob_relay_streaming::BlobRelayStreaming;
        using PutStreamingRequest = data_relay_grpc::proto::blob_relay::blob_relay_streaming::PutStreamingRequest;
        using PutStreamingResponse = data_relay_grpc::proto::blob_relay::blob_relay_streaming::PutStreamingResponse;

        auto channel = ::grpc::CreateChannel(endpoint_, ::grpc::InsecureChannelCredentials());
        BlobRelayStreaming::Stub stub(channel);
        ::grpc::ClientContext context{};

        PutStreamingResponse res{};
        std::unique_ptr<::grpc::ClientWriter<PutStreamingRequest>> writer(
            stub.Put(&context, &res));

        PutStreamingRequest req_metadata{};
        auto* metadata = req_metadata.mutable_metadata();
        metadata->set_api_version(BLOB_RELAY_API_VERSION);
        metadata->set_session_id(session_id);
        metadata->set_blob_size(data.size());

        LOG(INFO) << "[data_relay_client::put_blob] Metadata: "
                  << "api_version=" << metadata->api_version()
                  << " session_id=" << metadata->session_id()
                  << " blob_size=" << metadata->blob_size()
                  << " has_blob_size=" <<
                      (metadata->blob_size_opt_case() == PutStreamingRequest::Metadata::BlobSizeOptCase::kBlobSize);
        LOG(INFO) << "[data_relay_client::put_blob] req_metadata.payload_case()=" << req_metadata.payload_case()
                  << " (kMetadata=" << PutStreamingRequest::PayloadCase::kMetadata << ")"
                  << " has_metadata=" << req_metadata.has_metadata();
        LOG(INFO) << "[data_relay_client::put_blob] req_metadata.DebugString():\n"
                  << req_metadata.DebugString();

        if (! writer->Write(req_metadata)) {
            takatori::util::throw_exception(std::runtime_error(
                "put_blob: failed to write metadata"));
        }

        constexpr std::size_t chunk_size = 1024;
        std::size_t offset{0};
        std::size_t chunk_count{0};
        while (offset < data.size()) {
            std::size_t size = std::min(chunk_size, data.size() - offset);
            PutStreamingRequest req_chunk{};
            req_chunk.set_chunk(data.substr(offset, size));
            LOG(INFO) << "[data_relay_client::put_blob] Writing chunk " << chunk_count
                      << " offset=" << offset << " size=" << size;
            ++chunk_count;
            if (! writer->Write(req_chunk)) {
                takatori::util::throw_exception(std::runtime_error(
                    "put_blob: failed to write chunk at offset " + std::to_string(offset)));
            }
            offset += size;
        }

        bool writes_done_ok = writer->WritesDone();
        ::grpc::Status status = writer->Finish();

        if (!writes_done_ok) {
            LOG(ERROR) << "[data_relay_client::put_blob] WritesDone() failed (stream closed before completing writes)";
        }

        if (! status.ok()) {
            LOG(ERROR) << "[data_relay_client::put_blob] RPC failed, status=" << status.error_code()
                       << " message=" << status.error_message();
            if (res.has_blob()) {
                LOG(ERROR) << "[data_relay_client::put_blob] Response: object_id=" << res.blob().object_id()
                           << " storage_id=" << res.blob().storage_id();
            } else {
                LOG(ERROR) << "[data_relay_client::put_blob] Response has no blob field";
            }
            takatori::util::throw_exception(std::runtime_error(
                "put_blob: RPC failed, status=" + std::to_string(status.error_code()) +
                " message=" + status.error_message()));
        }
        LOG(INFO) << "[data_relay_client::put_blob] RPC finished successfully";
        LOG(INFO) << "[data_relay_client::put_blob] Response: object_id=" << res.blob().object_id()
                  << " storage_id=" << res.blob().storage_id();

        return {res.blob().object_id(), res.blob().storage_id()};
    }

private:
    std::string endpoint_{};
};

}  // namespace jogasaki::testing
