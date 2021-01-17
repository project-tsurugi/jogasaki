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

#include <vector>
#include <memory>
#include <takatori/util/optional_ptr.h>

#include <jogasaki/model/statement_kind.h>

namespace jogasaki {
class request_context;
}

namespace jogasaki::model {

template <class T>
using optional_ptr = takatori::util::optional_ptr<T>;

/**
 * @brief step graph represents the model of the requested statement
 */
class statement {
public:
    /**
     * @brief creates a new instance.
     */
    statement() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~statement() = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    statement(statement const& other) = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    statement& operator=(statement const& other) = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    statement(statement&& other) noexcept = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    statement& operator=(statement&& other) noexcept = default;

    [[nodiscard]] virtual statement_kind kind() const noexcept = 0;
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(statement const& a, statement const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(statement const& a, statement const& b) noexcept {
    return !(a == b);
}

}



