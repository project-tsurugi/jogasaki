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

#include <jogasaki/auth/authorized_users_action_set.h>
#include <jogasaki/auth/action_set.h>
#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::auth {

/**
 * @brief fill target authorized_users_action_set from the authorization list in TableDefinition.
 * @param tdef the source TableDefinition
 * @param target the target authorized_users_action_set to populate
 */
void from_authorization_list(
    proto::metadata::storage::TableDefinition const& tdef,
    authorized_users_action_set& target
);

/**
 * @brief fill target action_set from the default privilege in TableDefinition.
 * @param tdef the source TableDefinition
 * @param target the target action_set to populate
 */
void from_default_privilege(
    proto::metadata::storage::TableDefinition const& tdef,
    action_set& target
);

/**
 * @brief fill target authorization list and default privilege in TableDefinition
 * from given `users_actions` and `public_actions`.
 * @param users_actions the source users actions
 * @param public_actions the source public actions
 * @param target the target TableDefinition to populate
 */
void from_action_sets(
    authorized_users_action_set const& users_actions,
    action_set const& public_actions,
    proto::metadata::storage::TableDefinition& target
);

} // namespace jogasaki::auth
