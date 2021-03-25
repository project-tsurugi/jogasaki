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

#include <atomic>

#include <jogasaki/configuration.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/event_channel.h>

namespace jogasaki {

/**
 * @brief context object for the request scope
 * @details this class represents context information in the scope of the execution request
 */
class request_context {
public:
    /**
     * @brief create default context object
     */
    request_context();

    /**
     * @brief create new context object
     * @param ch channel used to communicate with scheduler
     * @param config global configuration
     * @param request_resource the memory resource used to construct request wide objects such as
     * processors and operators
     * @param database the kvs database shared within the request. Pass nullptr if the request doesn't access kvs.
     * @param result store to hold the result records, nullptr is allowed if the request doesn't create result set
     */
    request_context(
        std::shared_ptr<event_channel> ch,
        std::shared_ptr<class configuration> config,
        std::shared_ptr<memory::lifo_paged_memory_resource> request_resource = {},
        std::shared_ptr<kvs::database> database = {},
        std::shared_ptr<kvs::transaction> transaction = {},
        data::result_store* result = {}
    );

    /**
     * @brief accessor for the communication channel
     * @return channel to communicate with the scheduler handling the request
     */
    [[nodiscard]] std::shared_ptr<event_channel> const& channel() const;

    /**
     * @brief accessor for the gloabl configuration
     * @return global configuration
     */
    [[nodiscard]] std::shared_ptr<class configuration> const& configuration() const;

    /**
     * @brief accessor for the result store
     * @return result store
     */
    [[nodiscard]] data::result_store* result();

    /**
     * @brief accessor for the request resource
     * @return request wide memory resource
     */
    [[nodiscard]] memory::lifo_paged_memory_resource* request_resource() const noexcept;

    /**
     * @brief accessor for the database
     * @return database shared within this request
     */
    [[nodiscard]] std::shared_ptr<kvs::database> const& database() const;

    /**
     * @brief accessor for the transaction
     * @return transaction shared within this request
     */
    [[nodiscard]] std::shared_ptr<kvs::transaction> const& transaction() const;

    /**
     * @brief setter for the result status
     * @details this overwrites the status code with more severe status code (e.g. warning is overwritten by an error)
     * If error code is already set to this object, this is no-op.
     * @note this function is thread-safe and multiple threads can call this function concurrently
     */
    void status_code(status val) noexcept;

    /**
     * @brief accessor for the result status
     * @note this function is not thread-safe and should not be called concurrently with status_code(status val) above.
     */
    [[nodiscard]] status status_code() const noexcept;

private:
    std::shared_ptr<event_channel> channel_{};
    std::shared_ptr<class configuration> config_{};
    std::shared_ptr<memory::lifo_paged_memory_resource> request_resource_{};
    std::shared_ptr<kvs::database> database_{};
    std::shared_ptr<kvs::transaction> transaction_{};
    data::result_store* result_{};
    std::atomic<status> status_code_{status::ok};
};

}

