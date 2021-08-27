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

#include <yugawara/storage/table.h>
#include <yugawara/storage/index.h>

#include <jogasaki/configuration.h>
#include <jogasaki/status.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/transaction.h>

#include <tateyama/status.h>

namespace tateyama::api::endpoint {
class request;
class response;
}

namespace jogasaki::api {

class result_set;
class transaction;
class prepared_statement;
class statement_handle;
class executable_statement;
class parameter_set;

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 * @details this object is thread-safe and can be shared by multiple client threads.
 * The member functions except start() and stop() can be called from multiple threads simultaneously.
 * @note other objects in this public api are not thread-safe in general unless otherwise specified.
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
     * @brief start servicing database initializing tables, storages, or internal thread pools etc.
     * @details database initialization is done by this function.
     * No request should be made to database prior to this call.
     * @attention this function is not thread-safe. stop()/start() should be called from single thread at a time.
     * @return status::ok when successful
     * @return other code when error
     */
    virtual status start() = 0;

    /**
     * @brief stop servicing database
     * @details stopping database and closing internal resources.
     * No request should be made to the database after this call.
     * @attention this function is not thread-safe. stop()/start() should be called from single thread at a time.
     * @return status::ok when successful
     * @return other code when error
     */
    virtual status stop() = 0;

    /**
     * @brief register host variable
     * @param name the name of the host variable without colon
     * @param kind type kind of the host variable
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @deprecated This api will be removed in the future. The host variables definition should be passed
     * to prepare() function.
     */
    virtual status register_variable(std::string_view name, field_type_kind kind) = 0;

    /**
     * @brief prepare sql statement and create prepared statement
     * @details Prepared statement is the form of parsed statement with placeholders (not resolved.)
     * @param sql the sql text string to prepare
     * @param statement [out] the unique ptr to be filled with the created prepared statement
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @note the returned prepared statement can be shared by multiple threads.
     */
    virtual status prepare(
        std::string_view sql,
        std::unique_ptr<prepared_statement>& statement
    ) = 0;

    /**
     * @brief prepare sql statement and create prepared statement
     * @details Prepared statement is the form of parsed statement with placeholders (not resolved.)
     * This function stores the prepared statement internally and returns its handle, which must be released with
     * destoroy_statement() function when caller finishes using the statement.
     * @param sql the sql text string to prepare
     * @param statement [out] the handle to be filled with one for the created prepared statement
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @note the returned prepared statement can be shared by multiple threads.
     */
    virtual status prepare(
        std::string_view sql,
        statement_handle& statement
    ) = 0;

    /**
     * @brief prepare sql statement and create prepared statement
     * @details Prepared statement is the form of parsed statement with placeholders (not resolved.)
     * @param sql the sql text string to prepare
     * @param variables the placeholder variable name/type mapping
     * @param statement [out] the unique ptr to be filled with the created prepared statement
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @note the returned prepared statement can be shared by multiple threads.
     */
    virtual status prepare(std::string_view sql,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        std::unique_ptr<prepared_statement>& statement
    ) = 0;

    /**
     * @brief prepare sql statement and store prepared statement internally
     * @details Prepared statement is the form of parsed statement with placeholders (not resolved.)
     * This function stores the prepared statement internally and returns its handle, which must be released with
     * destoroy_statement() function when caller finishes using the statement.
     * @param sql the sql text string to prepare
     * @param variables the placeholder variable name/type mapping
     * @param statement [out] the handle to be filled with one for the created prepared statement
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @note the returned prepared statement can be shared by multiple threads.
     */
    virtual status prepare(std::string_view sql,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        statement_handle& statement
    ) = 0;

    /**
     * @brief destroy the prepared statement for the given handle
     * @details The internally stored prepared statement is released by this function.
     * After the success of this function, the handle becomes stale and it should not be used any more.
     * @param prepared the handle for the prepared statement to be destroyed
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     */
    virtual status destroy_statement(
        api::statement_handle prepared
    ) = 0;

    /**
     * @brief resolve the placeholder and create executable statement
     * @details Executable statement is the form of statement ready to execute, placeholders are resolved and
     * compilation is completed.
     * @param prepared the prepared statement used to create executable statement
     * @param parameters the parameters to assign value for each placeholder
     * @param statement [out] the unique ptr to be filled with the created executable statement
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @note the returned executable statement should be used from single thread/transaction at a point in time.
     */
    virtual status resolve(
        prepared_statement const& prepared,
        parameter_set const& parameters,
        std::unique_ptr<executable_statement>& statement
    ) = 0;

    /**
     * @brief resolve the placeholder and create executable statement
     * @details Executable statement is the form of statement ready to execute, placeholders are resolved and
     * compilation is completed.
     * @param prepared the prepared statement handle used to create executable statement
     * @param parameters the parameters to assign value for each placeholder
     * @param statement [out] the unique ptr to be filled with the created executable statement
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @note the returned executable statement should be used from single thread/transaction at a point in time.
     */
    virtual status resolve(
        statement_handle prepared,
        parameter_set const& parameters,
        std::unique_ptr<executable_statement>& statement
    ) = 0;

    /**
     * @brief prepare and create executable statement
     * @details this does prepare and resolve at once assuming no placeholder is used in the sql text
     * @param sql the sql text string to prepare
     * @param statement [out] the unique ptr to be filled with the created executable statement
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     * @note the returned executable statement should be used from single thread/transaction at a point in time.
     */
    virtual status create_executable(std::string_view sql,
        std::unique_ptr<executable_statement>& statement) = 0;

    /**
     * @brief explain the executable statement and dump the result to output stream
     * @param executable the executable statement to explain
     * @param out the output stream to write the result
     * @return status::ok when successful
     * @return other code when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     */
    virtual status explain(executable_statement const& executable, std::ostream& out) = 0;

    /**
     * @brief begin the new transaction
     * @param readonly specify whether the new transaction is read-only or not
     * @return transaction object when success
     * @return nullptr when error
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     */
    std::unique_ptr<transaction> create_transaction(bool readonly = false) {
        return do_create_transaction(readonly);
    }

    /**
     * @brief creates a table dump into the given output.
     * @param output the destination output stream
     * @param index_name the target index name
     * @param batch_size the max number of entries to be processed in each transaction,
     *    or 0 to process all entries in one transaction
     * @attention this function is not thread-safe. dump()/load() should be called from single thread at a time.
     */
    virtual void dump(std::ostream& output, std::string_view index_name, std::size_t batch_size) = 0;

    /**
     * @brief restores the table contents from dump() result.
     * @param input the source input stream
     * @param index_name the target index name
     * @param batch_size the max number of entries to be processed in each transaction,
     *    or 0 to process all entries in one transaction
     * @attention this function is not thread-safe. dump()/load() should be called from single thread at a time.
     */
    virtual void load(std::istream& input, std::string_view index_name, std::size_t batch_size) = 0;

    /**
     * @brief register table metadata
     * @param table the table to register
     * @param schema the schema where table belongs.
     * @return status::ok if the table is successfully created/registered.
     * @return status::err_already_exists if the table with same name already exists.
     * No update is made to the existing metadata.
     */
    status create_table(
        std::shared_ptr<yugawara::storage::table> table,
        std::string_view schema = {}
    ) {
        return do_create_table(std::move(table), schema);
    }

    /**
     * @brief find table metadata entry
     * @param name the table name to find
     * @param schema the schema where table belongs.
     * @return the table if found
     * @return nullptr otherwise
     */
    std::shared_ptr<yugawara::storage::table const> find_table(
        std::string_view name,
        std::string_view schema = {}
    ) {
        return do_find_table(name, schema);
    }

    /**
     * @brief unregister table metadata
     * @param name the table name to drop
     * @param schema the schema where table belongs.
     * @returns status::ok when the table is successfully dropped
     * @returns status::not_found when the table is not found
     * @note this doesn't modify the data stored in the table. Clean-up existing data needs to be done separately.
     */
    status drop_table(
        std::string_view name,
        std::string_view schema = {}
    ) {
        return do_drop_table(name, schema);
    }

    /**
     * @brief register index metadata
     * @param index the index to register
     * @param schema the schema where index belongs.
     * @return status::ok if the index is successfully created/registered.
     * @return status::err_already_exists if the table with same name already exists.
     */
    status create_index(
        std::shared_ptr<yugawara::storage::index> index,
        std::string_view schema = {}
    ) {
        return do_create_index(std::move(index), schema);
    }

    /**
     * @brief find index metadata entry
     * @param name the index name to find
     * @param schema the schema where index belongs.
     * @return the index if found
     * @return nullptr otherwise
     */
    std::shared_ptr<yugawara::storage::index const> find_index(
        std::string_view name,
        std::string_view schema = {}
    ) {
        return do_find_index(name, schema);
    }

    /**
     * @brief unregister index metadata
     * @param name the index name to drop
     * @param schema the schema where index belongs.
     * @returns status::ok when the index is successfully dropped
     * @returns status::not_found when the index is not found
     * @attention this function is not thread-safe, and should be called from single thread at a time.
     */
    status drop_index(
        std::string_view name,
        std::string_view schema = {}
    ) {
        return do_drop_index(name, schema);
    }

    /**
     * @brief register sequence metadata
     * @param sequence the sequence to register. Database-wide unique definition id must be assigned
     * for the sequence beforehand.
     * @param schema the schema where sequence belongs.
     * @return status::ok if the sequence is successfully created/registered.
     * @return status::err_already_exists if the table with same name already exists.
     */
    status create_sequence(
        std::shared_ptr<yugawara::storage::sequence> sequence,
        std::string_view schema = {}
    ) {
        return do_create_sequence(std::move(sequence), schema);
    }

    /**
     * @brief find sequence metadata entry
     * @param name the sequence name to find
     * @param schema the schema where sequence belongs.
     * @return the sequence if found
     * @return nullptr otherwise
     */
    std::shared_ptr<yugawara::storage::sequence const> find_sequence(
        std::string_view name,
        std::string_view schema = {}
    ) {
        return do_find_sequence(name, schema);
    }

    /**
     * @brief unregister sequence metadata
     * @param name the sequence name to drop
     * @param schema the schema where sequence belongs.
     * @returns status::ok when the sequence is successfully dropped
     * @returns status::not_found when the sequence is not found
     * @attention this function is not thread-safe, and should be called from single thread at a time.
     */
    status drop_sequence(
        std::string_view name,
        std::string_view schema = {}
    ) {
        return do_drop_sequence(name, schema);
    }

protected:
    virtual std::unique_ptr<transaction> do_create_transaction(bool readonly) = 0;

    virtual status do_create_table(
        std::shared_ptr<yugawara::storage::table> table,
        std::string_view schema
    ) = 0;

    virtual std::shared_ptr<yugawara::storage::table const> do_find_table(
        std::string_view name,
        std::string_view schema
    ) = 0;

    virtual status do_drop_table(
        std::string_view name,
        std::string_view schema
    ) = 0;

    virtual status do_create_index(
        std::shared_ptr<yugawara::storage::index> index,
        std::string_view schema
    ) = 0;

    virtual std::shared_ptr<yugawara::storage::index const> do_find_index(
        std::string_view name,
        std::string_view schema
    ) = 0;

    virtual status do_drop_index(
        std::string_view name,
        std::string_view schema
    ) = 0;

    virtual status do_create_sequence(
        std::shared_ptr<yugawara::storage::sequence> sequence,
        std::string_view schema
    ) = 0;

    virtual std::shared_ptr<yugawara::storage::sequence const> do_find_sequence(
        std::string_view name,
        std::string_view schema
    ) = 0;

    virtual status do_drop_sequence(
        std::string_view name,
        std::string_view schema
    ) = 0;
};

/**
 * @brief factory method for database
 * @param cfg configuration for the database
 * @return database api object
 * @return nullptr if error occurs on creation
 */
std::unique_ptr<database> create_database(std::shared_ptr<configuration> cfg = std::make_shared<configuration>());

}
