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

#include <memory>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/result_set.h>

namespace jogasaki::api {

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class transaction {
public:
    transaction() = default;
    virtual ~transaction() = default;
    transaction(transaction const& other) = delete;
    transaction& operator=(transaction const& other) = delete;
    transaction(transaction&& other) noexcept = delete;
    transaction& operator=(transaction&& other) noexcept = delete;

    virtual bool commit() = 0;
    virtual bool abort() = 0;
    virtual bool execute(executable_statement& statement) = 0;
    virtual bool execute(executable_statement& statement, std::unique_ptr<result_set>& result) = 0;
};

}
