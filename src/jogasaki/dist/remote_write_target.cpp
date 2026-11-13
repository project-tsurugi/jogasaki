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
#include "remote_write_target.h"

#include <takatori/plan/process.h>
#include <takatori/relation/write.h>
#include <takatori/statement/create_table.h>
#include <takatori/statement/create_index.h>
#include <takatori/statement/drop_table.h>
#include <takatori/statement/drop_index.h>
#include <takatori/statement/execute.h>
#include <takatori/statement/write.h>
#include <takatori/util/downcast.h>
#include <yugawara/binding/extract.h>
#include <yugawara/schema/provider.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki::dist {
namespace {
using takatori::util::unsafe_downcast;

bool remote_table(yugawara::storage::table const& table, remote_catalog_snapshot const& catalogs) {
    return catalogs.remote_definition(table) != nullptr || catalogs.remote_server(table) != nullptr;
}

bool remote_index(takatori::descriptor::relation const& descriptor, remote_catalog_snapshot const& catalogs) {
    auto index = yugawara::binding::extract_if<yugawara::storage::index>(descriptor);
    return index && (catalogs.remote_definition(*index) != nullptr || remote_table(index->table(), catalogs));
}

bool foreign_schema(takatori::descriptor::schema const& descriptor, remote_catalog_snapshot const& catalogs) {
    auto schema = yugawara::binding::extract_if<yugawara::schema::declaration>(descriptor);
    if (!schema) return false;
    bool remote{};
    for (auto const& catalog : catalogs.catalogs()) {
        catalog->schema_provider().each([&](auto const& candidate) {
            // The same provider can also be exposed through a copied table-only schema view.
            remote = remote || candidate.get() == schema.get() ||
                std::addressof(candidate->storage_provider()) == std::addressof(schema->storage_provider());
        });
        if (remote) break;
    }
    return remote;
}

bool remote_ddl(takatori::statement::statement const& statement, remote_catalog_snapshot const& catalogs) {
    using kind = takatori::statement::statement_kind;
    switch (statement.kind()) {
        case kind::create_table: {
            auto const& ddl = unsafe_downcast<takatori::statement::create_table>(statement);
            auto table = yugawara::binding::extract_if<yugawara::storage::table>(ddl.definition());
            return foreign_schema(ddl.schema(), catalogs) || (table && remote_table(*table, catalogs));
        }
        case kind::create_index: {
            auto const& ddl = unsafe_downcast<takatori::statement::create_index>(statement);
            return foreign_schema(ddl.schema(), catalogs) || remote_index(ddl.definition(), catalogs);
        }
        case kind::drop_table: {
            auto const& ddl = unsafe_downcast<takatori::statement::drop_table>(statement);
            auto table = yugawara::binding::extract_if<yugawara::storage::table>(ddl.target());
            return foreign_schema(ddl.schema(), catalogs) || (table && remote_table(*table, catalogs));
        }
        case kind::drop_index: {
            auto const& ddl = unsafe_downcast<takatori::statement::drop_index>(statement);
            return foreign_schema(ddl.schema(), catalogs) || remote_index(ddl.target(), catalogs);
        }
        default:
            return false;
    }
}

bool remote_dml(takatori::statement::statement const& statement, remote_catalog_snapshot const& catalogs) {
    using kind = takatori::statement::statement_kind;
    if (statement.kind() == kind::write) {
        return remote_index(unsafe_downcast<takatori::statement::write>(statement).destination(), catalogs);
    }
    if (statement.kind() != kind::execute) return false;
    auto const& execute = unsafe_downcast<takatori::statement::execute>(statement);
    for (auto const& step : execute.execution_plan()) {
        if (step.kind() != takatori::plan::step_kind::process) continue;
        for (auto const& relation : unsafe_downcast<takatori::plan::process>(step).operators()) {
            if (relation.kind() == takatori::relation::expression_kind::write &&
                remote_index(unsafe_downcast<takatori::relation::write>(relation).destination(), catalogs)) {
                return true;
            }
        }
    }
    return false;
}
} // namespace

std::shared_ptr<error::error_info> validate_remote_write_target(
    takatori::statement::statement const& statement, remote_catalog_snapshot const* catalogs) {
    if (!catalogs) return {};
    auto dml = remote_dml(statement, *catalogs);
    if (!dml && !remote_ddl(statement, *catalogs)) return {};
    LOG_LP(INFO) << "distributed query internal_code:" << (dml ? "REMOTE_DML_TARGET" : "REMOTE_DDL_TARGET")
                 << " statement_kind:" << statement.kind();
    return create_error_info(error_code::restricted_operation_exception,
        dml ? "[distributed-query] cannot modify a remote table"
            : "[distributed-query] cannot change a definition in a foreign catalog",
        status::err_illegal_operation);
}

} // namespace jogasaki::dist
