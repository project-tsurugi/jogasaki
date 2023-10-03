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

#include <takatori/statement/create_table.h>

#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::common {

/**
 * @brief create table statement
 */
class create_table : public model::statement {
public:

    /**
     * @brief create empty object
     */
    create_table() = default;

    /**
     * @brief create new object
     */
    explicit create_table(
        takatori::statement::create_table& ct
    ) noexcept;

    /**
     * @brief returns statement kind
     */
    [[nodiscard]] model::statement_kind kind() const noexcept override;

    /**
     * @brief execute body
     */
    bool operator()(request_context& context) const;

private:
    takatori::statement::create_table* ct_{};
};

}
