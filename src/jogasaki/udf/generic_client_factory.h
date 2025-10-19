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

#include <memory>
#include <string_view>

#include "generic_client.h"

#include <grpcpp/channel.h>
namespace plugin::udf {
class generic_client_factory {
public:

    generic_client_factory() = default;
    virtual ~generic_client_factory() = default;
    generic_client_factory(generic_client_factory const&) = delete;
    generic_client_factory& operator=(generic_client_factory const&) = delete;
    generic_client_factory(generic_client_factory&&) = delete;
    generic_client_factory& operator=(generic_client_factory&&) = delete;
    [[nodiscard]] virtual generic_client* create(std::shared_ptr<grpc::Channel> channel) const = 0;
};

extern "C" {
[[nodiscard]] generic_client_factory* tsurugi_create_generic_client_factory(char const* service_name);
void tsurugi_destroy_generic_client_factory(generic_client_factory* ptr);
void tsurugi_destroy_generic_client(generic_client* ptr);
}

}  // namespace plugin::udf
