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
#include <string>
#include <utility>

#include <grpcpp/grpcpp.h>
#include <jogasaki/test_utils/proto/blob_relay/blob_relay_streaming.grpc.pb.h>
#include <jogasaki/test_utils/proto/blob_relay/blob_relay_streaming.pb.h>

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
        using BlobRelayStreaming = data_relay_grpc::blob_relay::proto::BlobRelayStreaming;
        using GetStreamingRequest = data_relay_grpc::blob_relay::proto::GetStreamingRequest;
        using GetStreamingResponse = data_relay_grpc::blob_relay::proto::GetStreamingResponse;

        auto channel = ::grpc::CreateChannel(endpoint_, ::grpc::InsecureChannelCredentials());
        BlobRelayStreaming::Stub stub(channel);
        ::grpc::ClientContext context;

        GetStreamingRequest req;
        req.set_api_version(0);
        req.set_session_id(session_id);
        auto* blob = req.mutable_blob();
        blob->set_storage_id(storage_id);
        blob->set_object_id(blob_id);
        blob->set_tag(tag);

        std::unique_ptr<::grpc::ClientReader<GetStreamingResponse>> reader(
            stub.Get(&context, req));

        std::string blob_data{};
        GetStreamingResponse resp;
        while (reader->Read(&resp)) {
            blob_data += resp.chunk();
        }

        ::grpc::Status status = reader->Finish();
        if (! status.ok()) {
            return "";
        }

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
        using BlobRelayStreaming = data_relay_grpc::blob_relay::proto::BlobRelayStreaming;
        using PutStreamingRequest = data_relay_grpc::blob_relay::proto::PutStreamingRequest;
        using PutStreamingResponse = data_relay_grpc::blob_relay::proto::PutStreamingResponse;

        auto channel = ::grpc::CreateChannel(endpoint_, ::grpc::InsecureChannelCredentials());
        BlobRelayStreaming::Stub stub(channel);
        ::grpc::ClientContext context;

        PutStreamingResponse res;
        std::unique_ptr<::grpc::ClientWriter<PutStreamingRequest>> writer(
            stub.Put(&context, &res));

        PutStreamingRequest req_metadata;
        auto* metadata = req_metadata.mutable_metadata();
        metadata->set_api_version(0);
        metadata->set_session_id(session_id);
        if (! writer->Write(req_metadata)) {
            return {0, 0};
        }

        constexpr std::size_t chunk_size = 1024;
        PutStreamingRequest req_chunk;
        std::size_t offset = 0;
        while (offset < data.size()) {
            std::size_t size = std::min(chunk_size, data.size() - offset);
            req_chunk.set_chunk(data.data() + offset, size);
            if (! writer->Write(req_chunk)) {
                return {0, 0};
            }
            offset += size;
        }

        writer->WritesDone();
        ::grpc::Status status = writer->Finish();
        if (! status.ok()) {
            return {0, 0};
        }

        return {res.blob().object_id(), res.blob().storage_id()};
    }

private:
    std::string endpoint_{};
};

}  // namespace jogasaki::testing
