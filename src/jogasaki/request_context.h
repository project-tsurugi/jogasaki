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
#include <jogasaki/data/iteratable_record_store.h>
#include <jogasaki/plan/compiler_context.h>

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
     * @param result_store store to hold the result records, nullptr is allowed if the request doesn't create result set
     */
    request_context(std::shared_ptr<class channel> ch,
        std::shared_ptr<class configuration> config,
        std::shared_ptr<plan::compiler_context> compiler_context,
        std::shared_ptr<data::iteratable_record_store> result_store = {}
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
    [[nodiscard]] std::shared_ptr<data::iteratable_record_store> const& result_store() const;

    /**
     * @brief accessor for the compiler context
     * @return compiler context for this request
     */
    [[nodiscard]] std::shared_ptr<plan::compiler_context> const& compiler_context() const;

private:
    std::shared_ptr<class channel> channel_{};
    std::shared_ptr<class configuration> config_{};
    std::shared_ptr<plan::compiler_context> compiler_context_{};
    std::shared_ptr<data::iteratable_record_store> result_store_{};
};

}

