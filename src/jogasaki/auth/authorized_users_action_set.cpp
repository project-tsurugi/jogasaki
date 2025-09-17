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
#include <jogasaki/auth/authorized_users_action_set.h>

namespace jogasaki::auth {

void authorized_users_action_set::add_user_actions(std::string_view name, action_set actions) {
    if (actions.empty()) {
        return; // do nothing if no actions are provided
    }
    // merge actions with existing ones
    auto it = map_.find(std::string{name});
    if (it != map_.end()) {
        it->second.add_actions(actions);
        return;
    }
    map_.emplace(name, actions);
}

void authorized_users_action_set::remove_user_all_actions(std::string_view name) {
    map_.erase(std::string{name});
}

void authorized_users_action_set::remove_user_action(std::string_view name, action_kind action) {
    auto it = map_.find(std::string{name});
    if(it == map_.end()) {
        return;
    }
    it->second.remove_action(action);
    if(it->second.empty()) {
        map_.erase(it);
    }
}

void authorized_users_action_set::remove_user_actions(std::string_view name, action_set actions) {
    for(auto const& action : actions) {
        remove_user_action(name, action);
    }
}

action_set const& authorized_users_action_set::find_user_actions(std::string_view name) const {
    auto it = map_.find(std::string{name});
    if (it != map_.end()) {
        return it->second;
    }
    static constexpr action_set empty_set{};
    return empty_set;
}

}  // namespace jogasaki::auth
