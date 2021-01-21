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

#include <jogasaki/configuration.h>
#include <jogasaki/status.h>

/**
 * @brief SQL engine public API
 */
namespace jogasaki::api {

class result_set;
class transaction;
class prepared_statement;
class executable_statement;
class parameter_set;

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class database {
public:
    /**
     * @brief create empty object
     */
    database() = default;

    /**
     * @brief destruct the object
     */
    virtual ~database() = default;

    database(database const& other) = delete;
    database& operator=(database const& other) = delete;
    database(database&& other) noexcept = delete;
    database& operator=(database&& other) noexcept = delete;

    /**
     * @brief start servicing database initializing internal thread pools etc.
     */
    virtual status start() = 0;
    virtual status stop() = 0;

    virtual status prepare(std::string_view sql,
        std::unique_ptr<prepared_statement>& statement) = 0;

    virtual status create_executable(std::string_view sql,
        std::unique_ptr<executable_statement>& statement) = 0;

    virtual status resolve(
        prepared_statement const& prepared,
        parameter_set const& parameters,
        std::unique_ptr<executable_statement>& statement
    ) = 0;

    virtual status explain(executable_statement const& executable, std::ostream& out) = 0;

    virtual std::unique_ptr<transaction> do_create_transaction(bool readonly) = 0;

    std::unique_ptr<transaction> create_transaction(bool readonly = false) {
        return do_create_transaction(readonly);
    }
};

/**
 * @brief factory method for database
 * @param cfg configuration for the database
 * @return database api object
 * @return nullptr if error occurs on creation
 */
std::unique_ptr<database> create_database(std::shared_ptr<configuration> cfg = std::make_shared<configuration>());

}
