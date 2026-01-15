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

#include <cstddef>
#include <utility>

#include "generic_record.h"

#include <grpcpp/client_context.h>
namespace plugin::udf {
class generic_client {
public:

    using function_index_type = std::pair<int, int>;
    generic_client() = default;
    virtual ~generic_client() = default;
    generic_client(generic_client const&) = delete;
    generic_client& operator=(generic_client const&) = delete;
    generic_client(generic_client&&) = delete;
    generic_client& operator=(generic_client&&) = delete;

    virtual void call(
        grpc::ClientContext& context,
        function_index_type function_index,
        generic_record& request,
        generic_record& response
    ) const = 0;
    virtual std::unique_ptr<plugin::udf::generic_record_stream> call_server_streaming_async(
        std::unique_ptr<grpc::ClientContext> context,
        function_index_type function_index,
        generic_record& request
    ) const = 0;
};
}  // namespace plugin::udf