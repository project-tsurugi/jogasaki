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

/**
 * @brief SQL engine public API
 */
namespace jogasaki::api {

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class executable_statement {
public:
    executable_statement() = default;
    virtual ~executable_statement() = default;
    executable_statement(executable_statement const& other) = delete;
    executable_statement& operator=(executable_statement const& other) = delete;
    executable_statement(executable_statement&& other) noexcept = delete;
    executable_statement& operator=(executable_statement&& other) noexcept = delete;
};

}
