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

#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::executor::io {

/**
 * @brief group reader interface for the process to retrieve group data
 * @details The data will be presented as group (represented by key) entry and its members (represented by values).
 * The functions next_group/get_group are used to proceed the group position and to retrieve key corresponding
 * to the group.
 * The functions next_member/get_member are used to proceed the member position and to retrieve value corresponding
 * to the member.
 * At the beginning, initial group position is set just before the first group entry (if any).
 */
class group_reader {
public:
    /**
     * @brief move the current group position to the next group
     * @return true when next group entry exists and the position successfully moved forward
     * @return false when there is no next group
     * @pre either the following condition is met:
     * - no next_group() has been called since reader initialization
     * - most recent next_group() returned true, then next_member() has been called at least once,
     *   and most recent call returned false
     * @warning the function behavior is undefined when pre-condition stated above is not met
     */
    [[nodiscard]] virtual bool next_group() = 0;

    /**
     * @brief get the key corresponding to the current group
     * @return record reference to the key that represents the group on the current group position.
     * The returned record_ref will be invalidated when next_group() is called.
     * @pre all the following conditions are met:
     * - next_group() has been called at least once and most recent call returned true
     * - if next_member() has been called after most recent next_group(), the return value was true
     * @warning the function behavior is undefined when pre-condition stated above is not met
     */
    [[nodiscard]] virtual accessor::record_ref get_group() const = 0;

    /**
     * @brief move the current member position to the next member within current group
     * @return true when next member entry exists and the member position successfully moved forward
     * @return false when there is no next member
     * @pre all the following conditions are met:
     * - next_group() has been called at least once, and most recent call returned true
     * - if next_member() has been called after most recent next_group(), the return value was true
     * @warning the function behavior is undefined when pre-condition stated above is not met
     */
    [[nodiscard]] virtual bool next_member() = 0;

    /**
     * @brief get the value corresponding to the current member
     * @return record reference to the value that represents the member on the current member position.
     * The returned record_ref will be invalidated when next_member() is called.
     * @pre all the following conditions are met:
     * - next_group() has been called at least once, and most recent call returned true
     * - next_member() has been called after most recent next_group(), and the return value was true
     * @warning the function behavior is undefined when pre-condition stated above is not met
     */
    [[nodiscard]] virtual accessor::record_ref get_member() const = 0;

    /**
     * @brief declare ending use of this object and return to owner
     */
    virtual void release() = 0;

    /**
     * @brief creates a new instance.
     */
    group_reader() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~group_reader() = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    group_reader(group_reader const& other) = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    group_reader& operator=(group_reader const& other) = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    group_reader(group_reader&& other) noexcept = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    group_reader& operator=(group_reader&& other) noexcept = default;
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(group_reader const& a, group_reader const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(group_reader const& a, group_reader const& b) noexcept {
    return !(a == b);
}

}  // namespace jogasaki::executor::io
