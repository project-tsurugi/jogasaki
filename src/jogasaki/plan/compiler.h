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

#include <exception>
#include <memory>
#include <string_view>
#include <utility>

#include <takatori/plan/aggregate.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>
#include <takatori/statement/statement.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/compiled_info.h>
#include <yugawara/variable/configurable_provider.h>

#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/exchange/aggregate/step.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/plan/parameter_set.h>
#include <takatori/plan/step.h>
#include <jogasaki/status.h>

namespace jogasaki::plan {

using yugawara::compiled_info;

// for testing
namespace impl {

void preprocess(
    takatori::plan::process const& process,
    compiled_info const& info,
    std::shared_ptr<mirror_container> const& container
);

[[nodiscard]] std::shared_ptr<mirror_container> preprocess_mirror(
    maybe_shared_ptr<takatori::statement::statement> const& statement,
    std::shared_ptr<::yugawara::variable::configurable_provider> const& provider,
    compiled_info info
);

[[nodiscard]] executor::process::step create(
    takatori::plan::process const& process,
    compiled_info const& info,
    std::shared_ptr<mirror_container> const& mirrors,
    variable_table const* host_variables
);

[[nodiscard]] executor::exchange::forward::step create(takatori::plan::forward const& forward, compiled_info const& info);
[[nodiscard]] executor::exchange::group::step create(takatori::plan::group const& group, compiled_info const& info);
[[nodiscard]] executor::exchange::aggregate::step create(takatori::plan::aggregate const& agg, compiled_info const& info);

std::shared_ptr<executor::process::impl::variable_table_info> create_host_variable_info(
    std::shared_ptr<::yugawara::variable::configurable_provider> const& provider,
    compiled_info const& info
);

std::shared_ptr<executor::process::impl::variable_table> create_host_variables(
    parameter_set const* parameters,
    std::shared_ptr<executor::process::impl::variable_table_info> const& info
);
/**
 * @brief calculate partition for terminal
 * @param s the plan of step
 * @return size of the partition
 */
[[nodiscard]] size_t terminal_calculate_partition(takatori::plan::step const& s) noexcept;
/**
 * @brief calculate partition for intermediate
 * @param s the plan of step
 * @return size of the partition
 */
[[nodiscard]] size_t intermediate_calculate_partition(takatori::plan::step const& s) noexcept;
/**
 * @brief calculate partition
 * @param s the plan of step
 * @return size of the partition
 */
[[nodiscard]] size_t calculate_partition(takatori::plan::step const& s) noexcept;

/**
 * @brief determine whether to stop calculating the partition.
 * @param s the plan of step
 * @return true if stop calculate partition
 */
[[nodiscard]] bool stop_calculate_partition(takatori::plan::step const& s) noexcept;

}  // namespace impl

/**
 * @brief compile sql and store executable statement in the context
 * @param sql the sql statement to compile
 * @param ctx the compiler context filled with storage provider required to compile the sql
 * @param parameters parameters to resolve the place holder in the sql,
 * pass nullptr if place-holder resolution is not necessary.
 * @return status::ok when successful
 * @return any error and non-empty diagnostics is filled in ctx.error_info() object
 */
[[nodiscard]] status compile(std::string_view sql, compiler_context& ctx, parameter_set const* parameters = nullptr);

/**
 * @brief compile sql and store executable statement in the context
 * @param ctx the compiler context filled with prepared statement
 * @param parameters parameters to resolve the place holder in the sql,
 * pass nullptr if place-holder resolution is not necessary.
 * @return status::ok when successful
 * @return any error and non-empty diagnostics is filled in ctx.error_info() object
 */
[[nodiscard]] status compile(compiler_context &ctx, parameter_set const* parameters = nullptr);

/**
 * @brief pre-compile sql and store prepared statement in the context
 * @param sql the sql statement to compile
 * @param ctx the compiler context filled with storage provider required to pre-compile the sql
 * @return status::ok when successful
 * @return any error and non-empty diagnostics is filled in ctx.error_info() object
 */
[[nodiscard]] status prepare(std::string_view sql, compiler_context& ctx);

}  // namespace jogasaki::plan
