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

#include <glog/logging.h>
#include <sharksfin/api.h>
#include <sharksfin/Environment.h>

namespace jogasaki::storage {

class transaction_context;
/**
 * @brief context for the transactional storage engine
 */
class storage_context {
public:
    /**
     * @brief create default context object
     */
    storage_context() = default;

    /**
     * @brief create default context object
     */
    ~storage_context() noexcept;

    // TODO other constructors

    /**
     * @brief create default context object
     */
    bool open(std::map<std::string, std::string> const& options = {});

    bool close();

    [[nodiscard]] sharksfin::DatabaseHandle handle() const noexcept;

    std::shared_ptr<transaction_context> const& create_transaction();
private:
    sharksfin::DatabaseHandle db_{};
    std::vector<std::shared_ptr<transaction_context>> transactions_{};
};

}

