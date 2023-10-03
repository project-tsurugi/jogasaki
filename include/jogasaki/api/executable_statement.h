/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <jogasaki/api/record_meta.h>

/**
 * @brief SQL engine public API
 */
namespace jogasaki::api {

/**
 * @brief compiled, resolved statement ready to execute
 */
class executable_statement {
public:
    executable_statement() = default;
    virtual ~executable_statement() = default;
    executable_statement(executable_statement const& other) = delete;
    executable_statement& operator=(executable_statement const& other) = delete;
    executable_statement(executable_statement&& other) noexcept = delete;
    executable_statement& operator=(executable_statement&& other) noexcept = delete;

    /**
     * @brief accessor to output meta data
     * @return the record meta data if the statement has output data
     * @return nullptr otherwise
     */
    [[nodiscard]] virtual api::record_meta const* meta() const noexcept = 0;
};

}
