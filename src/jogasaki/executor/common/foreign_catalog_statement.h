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

#include <jogasaki/dist/remote_catalog_command.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/model/statement_kind.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::common {

/**
 * @brief Runtime CREATE FOREIGN CATALOG statement.
 * @details Owns the command values and executes against the request's database-owned catalog manager.
 * @note The command is the engine input until the formal Takatori statement becomes available.
 * @todo Construct this statement from the formal CREATE IR in plan::create_mirror_for_ddl().
 */
class create_foreign_catalog : public model::statement {
public:
    explicit create_foreign_catalog(dist::create_foreign_catalog_command command);
    [[nodiscard]] model::statement_kind kind() const noexcept override;
    bool operator()(request_context& context) const;

private:
    dist::create_foreign_catalog_command command_{};
};

/**
 * @brief Runtime DROP FOREIGN CATALOG statement.
 * @details Owns the command values; errors are reported through request_context as with existing DDL.
 * @todo Construct this statement from the formal DROP IR in plan::create_mirror_for_ddl().
 */
class drop_foreign_catalog : public model::statement {
public:
    explicit drop_foreign_catalog(dist::drop_foreign_catalog_command command);
    [[nodiscard]] model::statement_kind kind() const noexcept override;
    bool operator()(request_context& context) const;

private:
    dist::drop_foreign_catalog_command command_{};
};

} // namespace jogasaki::executor::common
