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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using executor::expr::evaluator_context;

/**
 * @brief definition of table-valued function type.
 * @details a table-valued function takes an evaluator context and a sequence of arguments,
 *          and returns a stream of any_sequence representing the result table.
 */
using table_valued_function_type = std::function<std::unique_ptr<data::any_sequence_stream>(
    evaluator_context& ctx,
    sequence_view<data::any>
)>;

/**
 * @brief represents information about a column in a table-valued function result.
 */
class table_valued_function_column {
public:
    /**
     * @brief constructs an empty column info.
     */
    table_valued_function_column() = default;

    /**
     * @brief constructs a new column info.
     * @param name the column name
     */
    explicit table_valued_function_column(std::string name);

    /**
     * @brief returns the column name.
     * @return the column name
     */
    [[nodiscard]] std::string const& name() const noexcept;

private:
    std::string name_{};
};

/**
 * @brief table-valued function information.
 * @details this class holds the metadata and implementation of a table-valued function.
 */
class table_valued_function_info {
public:
    using columns_type = std::vector<table_valued_function_column>;

    /**
     * @brief constructs an empty function info.
     */
    table_valued_function_info() = default;

    ~table_valued_function_info() = default;
    table_valued_function_info(table_valued_function_info const& other) = default;
    table_valued_function_info& operator=(table_valued_function_info const& other) = default;
    table_valued_function_info(table_valued_function_info&& other) noexcept = default;
    table_valued_function_info& operator=(table_valued_function_info&& other) noexcept = default;

    /**
     * @brief constructs a new function info.
     * @param kind the kind of the table-valued function
     * @param function_body the function implementation
     * @param arg_count the number of arguments the function takes
     * @param columns the output column definitions
     */
    table_valued_function_info(
        table_valued_function_kind kind,
        table_valued_function_type function_body,
        std::size_t arg_count,
        columns_type columns = {}
    );

    /**
     * @brief returns the kind of the table-valued function.
     * @return the function kind
     */
    [[nodiscard]] constexpr table_valued_function_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief returns the function implementation.
     * @return the function body
     */
    [[nodiscard]] table_valued_function_type const& function_body() const noexcept;

    /**
     * @brief returns the number of arguments.
     * @return the argument count
     */
    [[nodiscard]] std::size_t arg_count() const noexcept;

    /**
     * @brief returns the output column definitions.
     * @return the columns
     */
    [[nodiscard]] columns_type const& columns() const noexcept;

private:
    table_valued_function_kind kind_{};
    table_valued_function_type function_body_{};
    std::size_t arg_count_{};
    columns_type columns_{};
};

}  // namespace jogasaki::executor::function
