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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <jogasaki/executor/dto/common_column.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/request_statistics.h>

namespace tateyama::service_cli {

/**
 * @brief format the result set header in tgsql style, e.g. `[C0: INT, C1: VARCHAR(10)]`
 */
std::string format_header(std::vector<jogasaki::executor::dto::common_column> const& columns);

/**
 * @brief format the column type in tgsql style, e.g. `VARCHAR(10)`
 */
std::string format_type(jogasaki::executor::dto::common_column const& column);

/**
 * @brief format the record in tgsql style, e.g. `[1, abc, null]`
 */
std::string format_record(jogasaki::mock::basic_record const& record);

/**
 * @brief format the number of rows in tgsql style, e.g. `(1 row)`
 */
std::string format_row_count(std::size_t count);

/**
 * @brief format the statement result in tgsql style, e.g. `(1 row inserted)` or `execute succeeded`
 */
std::string format_statement_result(std::shared_ptr<jogasaki::request_statistics> const& stats);

}  // namespace tateyama::service_cli
