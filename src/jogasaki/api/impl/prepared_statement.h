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

#include <jogasaki/api/prepared_statement.h>

#include <jogasaki/plan/prepared_statement.h>

/**
 * @brief SQL engine public API
 */
namespace jogasaki::api::impl {

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class prepared_statement : public api::prepared_statement {
public:
    prepared_statement() = default;
    explicit prepared_statement(std::shared_ptr<plan::prepared_statement> body) : body_(std::move(body)) {}
    [[nodiscard]] std::shared_ptr<plan::prepared_statement> const& body() const noexcept {
        return body_;
    }
private:
    std::shared_ptr<plan::prepared_statement> body_{};
};

}
