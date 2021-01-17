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

#include <jogasaki/configuration.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/data/result_store.h>

namespace jogasaki {

class channel;

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
     * @param ch channel used for the scope objects to communicate with scheduler
     * @param config global configuration
     * @param request_resource the memory resource used to construct request wide objects such as processors and operators
     * @param database the kvs database shared within the request. Pass nullptr if the request doesn't access kvs.
     * @param result store to hold the result records, nullptr is allowed if the request doesn't create result set
     */
    request_context(std::shared_ptr<class channel> ch,
        std::shared_ptr<class configuration> config,
        std::unique_ptr<memory::lifo_paged_memory_resource> request_resource = {},
        std::shared_ptr<kvs::database> database = {},
        std::shared_ptr<kvs::transaction> transaction = {},
        data::result_store* result = {}
    );

    /**
     * @brief accessor for the communication channel
     * @return channel to communicate with the scheduler handling the request
     */
    [[nodiscard]] std::shared_ptr<class channel> const& channel() const;

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
    [[nodiscard]] memory::lifo_paged_memory_resource* request_resource() const noexcept {
        return request_resource_.get();
    }

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

private:
    std::shared_ptr<class channel> channel_{};
    std::shared_ptr<class configuration> config_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> request_resource_{};
    std::shared_ptr<kvs::database> database_{};
    std::shared_ptr<kvs::transaction> transaction_{};
    data::result_store* result_{};
};

}

