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
#include <functional>
#include <string>
#include <vector>

#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/value_store.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/udf/plugin_loader.h>
#include <jogasaki/udf/udf_loader.h>

#include <grpcpp/grpcpp.h>
namespace jogasaki::executor::function {
void add_udf_scalar_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    const std::vector<
        std::tuple<std::shared_ptr<plugin::udf::plugin_api>, std::shared_ptr<plugin::udf::generic_client>>>& plugins
);

class blob_grpc_metadata {
public:
    blob_grpc_metadata(const blob_grpc_metadata&) = delete;
    blob_grpc_metadata& operator=(const blob_grpc_metadata&) = delete;
    blob_grpc_metadata(blob_grpc_metadata&&) = delete;
    blob_grpc_metadata& operator=(blob_grpc_metadata&&) = delete;

    explicit blob_grpc_metadata(
        std::uint64_t session_id,
        std::string endpoint,
        bool secure,
        std::string transport,
        std::uint64_t chunk_size
    ) noexcept :
        session_id_(session_id),
        endpoint_(std::move(endpoint)),
        secure_(secure),
        transport_(std::move(transport)),
        chunk_size_(chunk_size) {}

    [[nodiscard]] bool apply(grpc::ClientContext& ctx) const noexcept;
    ~blob_grpc_metadata() noexcept = default;
    blob_grpc_metadata() = delete;
private:
    std::uint64_t session_id_{};
    std::string endpoint_{};
    bool secure_{false};
    std::string transport_{"stream"};
    const std::uint64_t chunk_size_{1048576};  // 1MB
};
}  // namespace jogasaki::executor::function
