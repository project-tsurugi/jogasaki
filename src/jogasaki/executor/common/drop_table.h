/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <takatori/statement/drop_table.h>

#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::common {

/**
 * @brief drop table statement
 */
class drop_table : public model::statement {
public:

    /**
     * @brief create empty object
     */
    drop_table() = default;

    /**
     * @brief create new object
     */
    explicit drop_table(
        takatori::statement::drop_table& ct
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
    takatori::statement::drop_table* ct_{};

};

}
