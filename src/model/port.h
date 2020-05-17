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
#include <takatori/util/sequence_view.h>

namespace jogasaki {


enum class port_direction {
    input,
    output,
};

enum class port_kind {
    main,
    sub,
};

}

namespace jogasaki::model {

template <class T>
using sequence_view = takatori::util::sequence_view<T>;


class step;

class port {
public:
    using opposites_type = std::vector<port*>;

    /**
     * @return the opposite ports connected with this port
     */
    [[nodiscard]] virtual sequence_view<port* const> opposites() const = 0;

    /**
     * @return kind of the port (main or sub)
     */
    [[nodiscard]] virtual port_kind kind() const = 0;

    /**
     * @return direction of the port (input or output)
     */
    [[nodiscard]] virtual port_direction direction() const = 0;

    /**
     * @return step that owns this port
     */
    [[nodiscard]] virtual step* const& owner() const = 0;

    /**
     * @brief set the owner step of this port
     */
    virtual void owner(step* arg) = 0;

    /**
     * @brief creates a new instance.
     */
    port() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~port() = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    port(port const& other) = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    port& operator=(port const& other) = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    port(port&& other) noexcept = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    port& operator=(port&& other) noexcept = default;
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(port const& a, port const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(port const& a, port const& b) noexcept {
    return !(a == b);
}

}

