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

#include <jogasaki/auth/action_kind.h>

namespace jogasaki::auth {

/**
 * @brief a set of authorized actions.
 * @note this object is not thread-safe, so only one thread should read/modify non-const object at a time.
 */
class action_set {
public:

    constexpr action_set() noexcept = default;

    /**
     * @brief constructs an action_set with any number of action_kind arguments.
     * @param args action_kind values to add to the set.
     */
    template<typename... Args>
    constexpr explicit action_set(Args... args) {
        (add_action(args), ...);
    }

    /**
     * @brief check if the specified action is allowed by the set.
     * @details If the action is `control`, it implies all actions and returns true.
     * @param arg The action to check.
     * @return true if the action is allowed, false otherwise.
     */
    [[nodiscard]] bool action_allowed(action_kind arg) const noexcept;

    /**
     * @brief check if the specified action exists in the set.
     * @details this simply checks if the action is in the set.
     * Even if the action is `control`, it does not imply all actions.
     * @param arg The action to check.
     * @return true if the action exists, false otherwise.
     */
    [[nodiscard]] bool has_action(action_kind arg) const noexcept;

    /**
     * @brief add the specified action to the set.
     * @details If the `arg` is `control`, it implies all actions and clears the other actions.
     * @param arg The action to add.
     */
    void add_action(action_kind arg);

    /**
     * @brief add the specified actions to the set.
     * @details If the `set` contains `control`, it implies all actions and clears the other actions.
     * @param set The actions to add.
     */
    void add_actions(action_set const& set);

    /**
     * @brief remove the specified action from the set.
     * @details If the set contains `control`, trying to remove individual actions does nothing.
     * @param arg The action to remove.
     */
    void remove_action(action_kind arg);

    /**
     * @brief check if the set is empty.
     * @return true if there are no actions in the set, false otherwise.
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief remove all actions from the set.
     */
    void clear();

    /**
     * @brief return a const iterator to the beginning of the set.
     */
    [[nodiscard]] auto begin() const noexcept {
        return actions_.begin();
    }

    /**
     * @brief return a const iterator to the end of the set.
     */
    [[nodiscard]] auto end() const noexcept {
        return actions_.end();
    }

    /**
     * @brief check if all actions in the given set are allowed by this set.
     * @param actions the set of actions to check.
     * @return true if all actions in `actions` are allowed by this set, false otherwise.
     */
    [[nodiscard]] bool allows(action_set const& actions) const noexcept;

    /**
     * @brief equality operator.
     * @return true if both sets contain the same actions.
     */
    friend bool operator==(action_set const& lhs, action_set const& rhs) noexcept {
        return lhs.actions_ == rhs.actions_;
    }

    /**
     * @brief inequality operator.
     * @return true if the sets contain different actions.
     */
    friend bool operator!=(action_set const& lhs, action_set const& rhs) noexcept {
        return ! (lhs == rhs);
    }

    friend std::ostream& operator<<(std::ostream& out, action_set const& value) {
        out << "action_set[";
        bool first = true;
        for(auto&& e : value.actions_) {
            if (! first) {
                out << ",";
            }
            first = false;
            out << e;
        }
        return out << "]";
    }
private:
    action_kind_set actions_;
};

}  // namespace jogasaki::auth
