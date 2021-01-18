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

class result_set;

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class database {
public:
    database();
    virtual ~database();
    database(database const& other) = delete;
    database& operator=(database const& other) = delete;
    database(database&& other) noexcept = delete;
    database& operator=(database&& other) noexcept = delete;

    virtual bool start() = 0;
    virtual bool stop() = 0;
    virtual bool execute(std::string_view sql) = 0;
    virtual bool execute(std::string_view sql, std::unique_ptr<result_set>& result) = 0;
};

/**
 * @brief factory method for database
 * @return Database for the current implementation
 */
std::unique_ptr<database> create_database();

}
