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



#include <memory>

#include <yugawara/schema/catalog.h>
#include <tateyama/grpc/distributed_query/service_adapter.h>

namespace jogasaki::api { class database; }

namespace jogasaki::dist {

/**
 * @brief the local Yugawara catalog that GetRemoteCatalog exposes.
 * @details The catalog default_catalog_name holds one schema public_schema_name over the local storage, regular
 * function and aggregate function providers, as SQL compilation sees them.
 * @return the catalog, or empty if the database has not been started
 */
[[nodiscard]] std::shared_ptr<::yugawara::schema::catalog const> local_catalog(api::database& database);

/**
 * @brief GetRemoteCatalog handler of the remote role.
 * @details Encodes local_catalog() through remote_catalog_encoder on each request and hands the complete serialized
 * catalog to Tateyama, which splits and streams it.
 */
class local_catalog_handler final : public tateyama::grpc::distributed_query::remote_catalog_handler {
public:
    explicit local_catalog_handler(api::database& database) noexcept;

    [[nodiscard]] tateyama::grpc::distributed_query::remote_catalog_result operator()() override;

private:
    api::database* database_{};
};

}  // namespace jogasaki::dist
