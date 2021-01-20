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

#include <string_view>
#include <memory>

/**
 * @brief SQL engine public API
 */
namespace jogasaki::api {

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class prepared_statement {
public:
    prepared_statement() = default;
    virtual ~prepared_statement() = default;
    prepared_statement(prepared_statement const& other) = delete;
    prepared_statement& operator=(prepared_statement const& other) = delete;
    prepared_statement(prepared_statement&& other) noexcept = delete;
    prepared_statement& operator=(prepared_statement&& other) noexcept = delete;
};

}
