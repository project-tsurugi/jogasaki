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
#include "local_catalog_handler.h"

#include <optional>
#include <string>
#include <utility>

#include <yugawara/schema/configurable_provider.h>
#include <yugawara/schema/declaration.h>
#include <tateyama/proto/distributed_query/remote_catalog.pb.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/constants.h>
#include <jogasaki/dist/remote_catalog_encoder.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

namespace dq = tateyama::grpc::distributed_query;

std::shared_ptr<::yugawara::schema::catalog const> local_catalog(api::database& database) {
    auto& impl = api::impl::get_impl(database);
    // The request retains its providers; bridge shutdown drains catalog calls before database stop.
    auto storage = impl.tables();
    auto functions = impl.regular_functions();
    auto aggregates = impl.aggregate_functions();
    if (!storage || !functions || !aggregates) {
        return {};
    }
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{
        std::nullopt, std::string{public_schema_name}, std::move(storage), {}, std::move(functions), std::move(aggregates)});
    return std::make_shared<yugawara::schema::catalog>(std::string{default_catalog_name}, std::nullopt, std::move(schemas));
}

local_catalog_handler::local_catalog_handler(api::database& database) noexcept :
    database_(std::addressof(database))
{}

dq::remote_catalog_result local_catalog_handler::operator()() {
    auto catalog = local_catalog(*database_);
    if (!catalog) {
        return {false, {}, "SQL engine is not started", dq::remote_catalog_error_kind::internal};
    }
    ::tateyama::grpc::proto::RemoteCatalog message{};
    std::string diagnostic{};
    if (auto rc = remote_catalog_encoder{}.encode(*catalog, message, diagnostic); rc != status::ok) {
        auto kind = rc == status::err_unsupported ?
            dq::remote_catalog_error_kind::unsupported : dq::remote_catalog_error_kind::internal;
        return {false, {}, std::move(diagnostic), kind};
    }
    dq::remote_catalog_result result{true, {}, {}};
    if (!message.SerializeToString(&result.catalog)) {
        return {false, {}, "cannot serialize remote catalog", dq::remote_catalog_error_kind::internal};
    }
    return result;
}

}  // namespace jogasaki::dist
