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
#include <memory>
#include <vector>

#include <jogasaki/udf/generic_client_factory.h>
#include <jogasaki/udf/plugin_api.h>

namespace plugin::udf {
namespace {
class lifecycle_api : public plugin_api {
public:
    std::vector<package_descriptor*> const& packages() const noexcept override { return packages_; }
private:
    std::vector<package_descriptor*> packages_{};
};
class lifecycle_client : public generic_client {
public:
    void call(generic_client_context&, function_index_type, generic_record&, generic_record&) const override {}
    std::unique_ptr<generic_record_stream> call_server_streaming_async(
        std::unique_ptr<generic_client_context>, function_index_type, generic_record&) const override { return {}; }
};
class lifecycle_factory : public generic_client_factory {
public:
    generic_client* create(std::shared_ptr<grpc::Channel>) const override { return new lifecycle_client{}; }
};
}
extern "C" plugin_api* create_plugin_api() { return new lifecycle_api{}; }
extern "C" generic_client_factory* tsurugi_create_generic_client_factory(char const*) { return new lifecycle_factory{}; }
}
