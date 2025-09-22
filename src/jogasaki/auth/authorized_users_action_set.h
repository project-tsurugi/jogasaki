/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <unordered_map>

#include <jogasaki/auth/action_set.h>

namespace jogasaki::auth {

/**
 * @brief represent the set of authorized users and their actions
 * @note this object is not thread-safe, so only one thread should read/modify non-const object at a time.
 */
class authorized_users_action_set {
public:
    using entity_type = std::unordered_map<std::string, action_set>;

    /**
     * @brief Add authorized actions for a user.
     * @param name The user name.
     * @param actions The set of actions to add for the user.
     */
    void add_user_actions(std::string_view name, action_set actions);

    /**
     * @brief Remove all authorized actions for a user.
     * @details Does nothing if the user has not been authorized yet.
     * @param name The user name to remove all actions from.
     */
    void remove_user_all_actions(std::string_view name);

    /**
     * @brief Remove a specific authorized action from a user.
     * @param name The user name.
     * @param action The action to remove from the user's authorized actions.
     * @details If the user or action does not exist, does nothing. If the actions for a user become empty,
     * the user entry is removed.
     */
    void remove_user_action(std::string_view name, action_kind action);

    /**
     * @brief remove authorized actions for a user.
     * @param name The user name.
     * @param actions The set of actions to remove from the user.
     */
    void remove_user_actions(std::string_view name, action_set actions);

    /**
     * @brief Find the authorized actions for a user.
     * @param name The user name to find.
     * @return The action set for the user, or an empty set if the user entry is not found.
     */
    [[nodiscard]] action_set const& find_user_actions(std::string_view name) const;

    /**
     * @brief Get an iterator to the beginning of the authorized users.
     * @return An iterator to the beginning of the authorized users.
     */
    auto begin() const noexcept {
        return map_.begin();
    }

    /**
     * @brief Get an iterator to the beginning of the authorized users.
     * @return An iterator to the beginning of the authorized users.
     */
    auto begin() noexcept {
        return map_.begin();
    }

    /**
     * @brief Get an iterator to the end of the authorized users.
     * @return An iterator to the end of the authorized users.
     */
    auto end() const noexcept {
        return map_.end();
    }

    /**
     * @brief Get an iterator to the end of the authorized users.
     * @return An iterator to the end of the authorized users.
     */
    auto end() noexcept {
        return map_.end();
    }

    /**
     * @brief clear all authorized users and their actions.
     */
    void clear() noexcept {
        map_.clear();
    }

    /**
     * @brief erase the element at the iterator position
     * @returns interator at the next position
     */
    auto erase(entity_type::iterator it) noexcept {
        return map_.erase(it);
    }

    /**
     * @brief return the number of authorized users.
     */
    [[nodiscard]] std::size_t user_count() const noexcept {
        return map_.size();
    }

private:
    entity_type map_{};
};

}  // namespace jogasaki::auth
