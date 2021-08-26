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

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/statement/statement.h>
#include <yugawara/compiled_info.h>
#include <yugawara/variable/configurable_provider.h>
#include <jogasaki/plan/mirror_container.h>

namespace jogasaki::plan {

using ::takatori::util::maybe_shared_ptr;

/**
 * @brief prepared statement
 */
class prepared_statement {
public:
    /**
     * @brief create empty object
     */
    prepared_statement() = default;

    /**
     * @brief destruct the object
     */
    ~prepared_statement() = default;

    prepared_statement(prepared_statement const& other) = delete;
    prepared_statement& operator=(prepared_statement const& other) = delete;
    prepared_statement(prepared_statement&& other) noexcept = default;
    prepared_statement& operator=(prepared_statement&& other) noexcept = default;

    /**
     * @brief create new object
     */
    prepared_statement(
        maybe_shared_ptr<::takatori::statement::statement> statement,
        yugawara::compiled_info compiled_info,
        std::shared_ptr<::yugawara::variable::configurable_provider> host_variables,
        mirror_container mirrors
    ) noexcept :
        statement_(std::move(statement)),
        compiled_info_(std::move(compiled_info)),
        host_variables_(std::move(host_variables)),
        mirrors_(std::move(mirrors))
    {}

    [[nodiscard]] maybe_shared_ptr<::takatori::statement::statement> const& statement() const noexcept {
        return statement_;
    }

    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept {
        return compiled_info_;
    }

    [[nodiscard]] mirror_container const& mirrors() const noexcept {
        return mirrors_;
    }

    [[nodiscard]] std::shared_ptr<yugawara::variable::configurable_provider> const& host_variables() const noexcept {
        return host_variables_;
    }
private:
    maybe_shared_ptr<::takatori::statement::statement> statement_{};
    yugawara::compiled_info compiled_info_{};
    std::shared_ptr<yugawara::variable::configurable_provider> host_variables_{};
    mirror_container mirrors_{};
};

}
