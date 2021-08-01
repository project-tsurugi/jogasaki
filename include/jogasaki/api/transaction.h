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
#include <jogasaki/api/prepared_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/status.h>

namespace jogasaki::api {

/**
 * @brief interface to execute statement in the transaction, or to finish the transaction
 */
class transaction {
public:
    /**
     * @brief create new object
     */
    transaction() = default;

    /**
     * @brief destruct the object
     */
    virtual ~transaction() = default;

    transaction(transaction const& other) = delete;
    transaction& operator=(transaction const& other) = delete;
    transaction(transaction&& other) noexcept = delete;
    transaction& operator=(transaction&& other) noexcept = delete;

    /**
     * @brief commit the transaction
     * @return status::ok when successful
     * @return error code otherwise
     */
    virtual status commit() = 0;

    /**
     * @brief abort the transaction and have transaction engine rollback the on-going processing (if it supports rollback)
     * @return status::ok when successful
     * @return error code otherwise
     */
    virtual status abort() = 0;

    /**
     * @brief execute the statement in the transaction. No result records are expected
     * from the statement (e.g. insert/update/delete).
     * @param statement the statement to be executed
     * @return status::ok when successful
     * @return error code otherwise
     */
    virtual status execute(executable_statement& statement) = 0;

    /**
     * @brief execute the statement in the transaction. The result records are expected.
     * from the statement (e.g. query to tables/views).
     * @param statement the statement to be executed
     * @param result [out] the unique ptr to be filled with result set, which must be closed when caller
     * completes using the result records.
     * @return status::ok when successful
     * @return error code otherwise
     */
    virtual status execute(executable_statement& statement, std::unique_ptr<result_set>& result) = 0;

    /**
     * @brief resolve the placeholder and execute the prepared statement
     */
    virtual status execute(
        prepared_statement const& prepared,
        parameter_set const& parameters
    ) = 0;

    /**
     * @brief resolve the placeholder and execute the prepared statement
     */
    virtual status execute(
        prepared_statement const& prepared,
        parameter_set const& parameters,
        std::unique_ptr<result_set>& result
    ) = 0;
};

}
