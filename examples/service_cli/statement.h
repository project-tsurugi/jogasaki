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

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>

#include "service_client.h"

namespace tateyama::service_cli {

/**
 * @brief statement parsed from the statement text
 */
struct statement {
    enum class kind {
        /// @brief sql statement sent to the server as it is
        sql,

        /// @brief BEGIN / START TRANSACTION
        begin,

        /// @brief COMMIT
        commit,

        /// @brief ROLLBACK
        rollback,

        /// @brief EXPLAIN <sql>
        explain,

        /// @brief CALL (not supported)
        call,

        /// @brief `\prepare <name> [<var>:<type> ...] AS <sql>`
        prepare,

        /// @brief `\execute <name> [<var>=<value> ...]`
        execute,

        /// @brief special statements starting with backslash (other than prepare/execute)
        special,
    };

    kind kind_{};  //NOLINT

    /// @brief the original text
    std::string text_{};  //NOLINT

    /// @brief transaction options for begin
    jogasaki::common_cli::begin_options begin_{};  //NOLINT

    /// @brief commit status for commit
    jogasaki::proto::sql::request::CommitStatus commit_status_{};  //NOLINT

    /// @brief the sql text for explain and prepare
    std::string sql_{};  //NOLINT

    /// @brief statement name for prepare/execute, or command name (lower case without backslash) for special
    std::string name_{};  //NOLINT

    /// @brief placeholder variables for prepare
    std::vector<std::pair<std::string, jogasaki::proto::sql::common::AtomType>> variables_{};  //NOLINT

    /// @brief parameter values for execute. nullopt means NULL.
    std::vector<std::pair<std::string, std::optional<std::string>>> parameters_{};  //NOLINT

    /// @brief arguments for special statement
    std::vector<std::string> arguments_{};  //NOLINT
};

/**
 * @brief parse the statement text
 * @param text the statement text without delimiter
 * @param out filled with the parsed result
 * @return error message, or empty if successful
 */
std::string parse_statement(std::string_view text, statement& out);

/**
 * @brief returns the atom type for the sql type name used in `\prepare`
 * @return TYPE_UNSPECIFIED if the name is not supported
 */
jogasaki::proto::sql::common::AtomType atom_type_of(std::string_view type_name);

}  // namespace tateyama::service_cli
