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
#include <takatori/statement/statement.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/error/error_info.h>

namespace jogasaki::dist {

/**
 * @brief Reject resolved DML targets and table/index DDL in a remote catalog.
 * @details Uses retained declaration/provider identity, never SQL text or target names.
 * Called after successful compilation and before runtime storage registration.
 * Compiler/analyzer failures remain their existing errors. This is not foreign-catalog DDL.
 * @return M34/M35 error information, or empty when no remote update target is present.
 */
[[nodiscard]] std::shared_ptr<error::error_info> validate_remote_write_target(
    takatori::statement::statement const& statement, remote_catalog_snapshot const* catalogs);

} // namespace jogasaki::dist
