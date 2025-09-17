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
#include <jogasaki/auth/action_set.h>

#include <algorithm>

namespace jogasaki::auth {

bool action_set::action_allowed(action_kind arg) const noexcept {
    // if actions_ contains control, then any action is allowed
    if (actions_.contains(action_kind::control)) {
        return true;
    }
    return actions_.contains(arg);
}

void action_set::add_action(action_kind arg) {
    if (arg == action_kind::control) {
        actions_.clear();
        actions_.insert(arg);
        return;
    }
    // if control is already in the set, then we do not add other actions
    if (actions_.contains(action_kind::control)) {
        return;
    }
    actions_.insert(arg);
}

void action_set::add_actions(action_set const& set) {
    for(auto &&s : set.actions_) {
        add_action(s);
    }
}

void action_set::remove_action(action_kind arg) {
    if (arg == action_kind::control) {
        actions_.clear();
        return;
    }
    actions_.erase(arg);
}

void action_set::remove_actions(action_set const& set) {
    for(auto &&s : set.actions_) {
        remove_action(s);
    }
}

bool action_set::empty() const noexcept {
    return actions_.empty();
}

void action_set::clear() {
    actions_.clear();
}

bool action_set::has_action(action_kind arg) const noexcept {
    return actions_.contains(arg);
}

bool action_set::allows(action_set const& actions) const noexcept {
    return std::all_of(actions.actions_.begin(), actions.actions_.end(), [this](action_kind a) {
        return action_allowed(a);
    });
}

} // namespace jogasaki::auth
