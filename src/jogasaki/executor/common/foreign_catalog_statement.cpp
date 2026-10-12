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
#include "foreign_catalog_statement.h"

#include <string>
#include <utility>

#include <jogasaki/dist/remote_catalog_command.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/status.h>

namespace jogasaki::executor::common {

static bool report_catalog_result(request_context& context, status result, std::string const& diagnostic) {
    if (result == status::ok) {
        return true;
    }
    auto code = error_code::sql_execution_exception;
    if (result == status::err_already_exists) {
        code = error_code::target_already_exists_exception;
    } else if (result == status::err_not_found) {
        code = error_code::target_not_found_exception;
    } else if (result == status::err_serialization_failure) {
        code = error_code::cc_exception;
    }
    set_error_context(context, code,
        diagnostic.empty() ? "foreign catalog operation failed" : diagnostic, result);
    return false;
}

create_foreign_catalog::create_foreign_catalog(dist::create_foreign_catalog_command command)
    : command_(std::move(command)) {}

model::statement_kind create_foreign_catalog::kind() const noexcept {
    return model::statement_kind::create_foreign_catalog;
}

bool create_foreign_catalog::operator()(request_context& context) const {
    auto const& catalogs = context.remote_catalogs_manager();
    auto const* channels = context.remote_server_channels();
    if (!catalogs || !channels) {
        return report_catalog_result(context, status::err_invalid_state,
            "remote catalog manager or channels are not available");
    }
    std::string diagnostic{};
    auto result = dist::execute_create_foreign_catalog(command_, *catalogs, *channels, diagnostic);
    return report_catalog_result(context, result, diagnostic);
}

drop_foreign_catalog::drop_foreign_catalog(dist::drop_foreign_catalog_command command)
    : command_(std::move(command)) {}

model::statement_kind drop_foreign_catalog::kind() const noexcept {
    return model::statement_kind::drop_foreign_catalog;
}

bool drop_foreign_catalog::operator()(request_context& context) const {
    auto const& catalogs = context.remote_catalogs_manager();
    if (!catalogs) {
        return report_catalog_result(context, status::err_invalid_state,
            "remote catalog manager is not available");
    }
    std::string diagnostic{};
    auto result = dist::execute_drop_foreign_catalog(command_, *catalogs, diagnostic);
    return report_catalog_result(context, result, diagnostic);
}

} // namespace jogasaki::executor::common
