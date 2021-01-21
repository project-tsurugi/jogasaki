/*
 * Copyright 2018-2020 tsurugi project.
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

#include <takatori/util/object_creator.h>
#include <yugawara/compiler_result.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::plan {

/**
 * @brief executable statement
 */
class cache_align executable_statement {
public:

    executable_statement() = default;

    executable_statement(
        takatori::util::unique_object_ptr<::takatori::statement::statement> statement,
        yugawara::compiled_info compiled_info,
        std::shared_ptr<model::statement> operators
    ) noexcept;

    void compiler_result(yugawara::compiler_result compiler_result) noexcept;

    void statement(std::unique_ptr<::takatori::statement::statement> statement) noexcept;

    [[nodiscard]] ::takatori::statement::statement const& statement() const noexcept;

    void compiled_info(yugawara::compiled_info compiled_info) noexcept;

    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept;

    void operators(std::shared_ptr<model::statement> operators) noexcept;

    [[nodiscard]] model::statement const* operators() const noexcept;

    [[nodiscard]] bool is_execute() const noexcept;

private:
    takatori::util::unique_object_ptr<::takatori::statement::statement> statement_{};
    yugawara::compiled_info compiled_info_{};
    std::shared_ptr<model::statement> operators_{};
};

}
