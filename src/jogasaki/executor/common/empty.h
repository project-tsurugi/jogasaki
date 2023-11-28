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

#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::common {

/**
 * @brief empty statement
 */
class empty : public model::statement {
public:

    /**
     * @brief create empty object
     */
    empty() = default;

    /**
     * @brief returns statement kind
     */
    [[nodiscard]] model::statement_kind kind() const noexcept override {
        return model::statement_kind::empty;
    }

    /**
     * @brief execute body
     */
    bool operator()(request_context& context) const {
        (void) context;
        return true;
    }
};

}