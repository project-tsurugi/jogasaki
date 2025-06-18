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

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/transaction_context.h>

#include <takatori/plan/step.h>

namespace jogasaki::executor {
namespace impl {

/**
 * @brief determine whether to stop calculating the partition.
 * @param s the plan of step
 * @return true if stop calculate partition
 */
bool has_emit_operator(takatori::plan::step const& s) noexcept;

/**
 * @brief calculate partition for terminal
 * @param s the plan of step
 * @param partitions the number of partitions
 * @param is_rtx transaction is read-only or not
 * @return size of the partition

 */
size_t terminal_calculate_partition(
    takatori::plan::step const& s, size_t partitions, bool is_rtx) noexcept;

/**
 * @brief calculate partition for intermediate
 * @param s the plan of step
 * @param partitions the number of partitions
 * @param is_rtx transaction is read-only or not
 * @details "scan" and "find" do not appear in the same location.
 * @see https://github.com/project-tsurugi/takatori/blob/master/docs/ja/execution-model.md
 * @return size of the partition
 */
size_t intermediate_calculate_partition(
    takatori::plan::step const& s, size_t partitions, bool is_rtx) noexcept;

/**
 * @brief calculate partition
 * @param s the plan of step
 * @param partitions the number of partitions
 * @param is_rtx transaction is read-only or not
 * @return the computed partition size.
 */
size_t calculate_partition(takatori::plan::step const& s, size_t partitions, bool is_rtx) noexcept;
} // namespace impl
/**
 * @brief Calculate the maximum number of writers for a given statement.
 * @param stmt the executable statement
 * @param tx the transaction context
 * @return the maximum number of writers
 */
[[nodiscard]] std::size_t calculate_max_writer_count(
    api::executable_statement const& stmt, transaction_context const& tx);

} // namespace jogasaki::executor
