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
#include <jogasaki/auth/fill_action_set.h>
#include <jogasaki/auth/action_kind.h>

namespace jogasaki::auth {

namespace {

action_kind from(proto::metadata::storage::TableActionKind kind) {
    using proto::metadata::storage::TableActionKind;
    using jogasaki::auth::action_kind;
    switch(kind) {
        case TableActionKind::CONTROL: return action_kind::control;
        case TableActionKind::SELECT: return action_kind::select;
        case TableActionKind::INSERT: return action_kind::insert;
        case TableActionKind::UPDATE: return action_kind::update;
        case TableActionKind::DELETE: return action_kind::delete_;
        default: return action_kind::undefined;
    }
    std::abort();
}

proto::metadata::storage::TableActionKind from(action_kind kind) {
    using proto::metadata::storage::TableActionKind;
    switch(kind) {
        case action_kind::control: return TableActionKind::CONTROL;
        case action_kind::select: return TableActionKind::SELECT;
        case action_kind::insert: return TableActionKind::INSERT;
        case action_kind::update: return TableActionKind::UPDATE;
        case action_kind::delete_: return TableActionKind::DELETE;
        default: return TableActionKind::UNSPECIFIED;
    }
    std::abort();
}

} // anonymous namespace

void from_authorization_list(
    proto::metadata::storage::TableDefinition const& tdef,
    authorized_users_action_set& target
) {
    for(auto const& e : tdef.authorization_list()) {
        action_set actions{};
        for(auto const& p : e.privilege_list()) {
            actions.add_action(from(p.action_kind()));
        }
        if (! actions.empty()) {
            target.add_user_actions(e.identifier(), actions);
        }
    }
}

void from_default_privilege(
    proto::metadata::storage::TableDefinition const& tdef,
    action_set& target
) {
    for(auto const& e : tdef.default_privilege_list()) {
        target.add_action(from(e.action_kind()));
    }
}

void from_action_sets(
    authorized_users_action_set const& users_actions,
    action_set const& public_actions,
    proto::metadata::storage::TableDefinition& target
) {
    target.clear_authorization_list();
    target.clear_default_privilege_list();

    for (auto const& entry : users_actions) {
        auto* auth = target.add_authorization_list();
        auth->set_identifier(entry.first);
        for (auto const& act : entry.second) {
            auto* priv = auth->add_privilege_list();
            priv->set_action_kind(from(act));
        }
    }

    for (auto const& act : public_actions) {
        auto* priv = target.add_default_privilege_list();
        priv->set_action_kind(from(act));
    }
}

} // namespace jogasaki::auth
