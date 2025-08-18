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

#include <gtest/gtest.h>

#include <jogasaki/auth/action_kind.h>
#include <jogasaki/auth/authorized_users_action_set.h>
#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::auth {

class fill_action_set_test : public ::testing::Test {
protected:

    proto::metadata::storage::TableDefinition tdef_{};
    authorized_users_action_set target_{};
};

TEST_F(fill_action_set_test, empty_authorization_list) {
    // No authorization entries
    from_authorization_list(tdef_, target_);
    auto const& found = target_.find_user_actions("user");
    EXPECT_TRUE(found.empty());
}

TEST_F(fill_action_set_test, single_user_single_action) {
    auto* entry = tdef_.add_authorization_list();
    entry->set_identifier("user");
    auto* priv = entry->add_privilege_list();
    priv->set_action_kind(proto::metadata::storage::TableActionKind::SELECT);

    from_authorization_list(tdef_, target_);
    auto const& found = target_.find_user_actions("user");
    EXPECT_TRUE(found.has_action(action_kind::select));
    EXPECT_TRUE(! found.has_action(action_kind::insert));
}

TEST_F(fill_action_set_test, single_user_multiple_actions) {
    auto* entry = tdef_.add_authorization_list();
    entry->set_identifier("user");
    auto* priv1 = entry->add_privilege_list();
    priv1->set_action_kind(proto::metadata::storage::TableActionKind::SELECT);
    auto* priv2 = entry->add_privilege_list();
    priv2->set_action_kind(proto::metadata::storage::TableActionKind::INSERT);

    from_authorization_list(tdef_, target_);
    auto const& found = target_.find_user_actions("user");
    EXPECT_TRUE(found.has_action(action_kind::select));
    EXPECT_TRUE(found.has_action(action_kind::insert));
    EXPECT_TRUE(! found.has_action(action_kind::update));
}

TEST_F(fill_action_set_test, multiple_users) {
    auto* entry1 = tdef_.add_authorization_list();
    entry1->set_identifier("userA");
    entry1->add_privilege_list()->set_action_kind(proto::metadata::storage::TableActionKind::SELECT);

    auto* entry2 = tdef_.add_authorization_list();
    entry2->set_identifier("userB");
    entry2->add_privilege_list()->set_action_kind(proto::metadata::storage::TableActionKind::INSERT);

    from_authorization_list(tdef_, target_);
    auto const& foundA = target_.find_user_actions("userA");
    auto const& foundB = target_.find_user_actions("userB");
    EXPECT_TRUE(foundA.has_action(action_kind::select));
    EXPECT_TRUE(! foundA.has_action(action_kind::insert));
    EXPECT_TRUE(foundB.has_action(action_kind::insert));
    EXPECT_TRUE(! foundB.has_action(action_kind::select));
}

TEST_F(fill_action_set_test, ignores_empty_actions) {
    auto* entry = tdef_.add_authorization_list();
    entry->set_identifier("user");
    // No privileges added

    from_authorization_list(tdef_, target_);
    auto const& found = target_.find_user_actions("user");
    EXPECT_TRUE(found.empty());
}

TEST_F(fill_action_set_test, control_and_select_results_in_only_control) {
    // control contains select, so even if serialized format contains both,
    // only control should remain in the target action_set.
    auto* entry = tdef_.add_authorization_list();
    entry->set_identifier("user");
    auto* priv_control = entry->add_privilege_list();
    priv_control->set_action_kind(proto::metadata::storage::TableActionKind::CONTROL);
    auto* priv_select = entry->add_privilege_list();
    priv_select->set_action_kind(proto::metadata::storage::TableActionKind::SELECT);

    from_authorization_list(tdef_, target_);
    auto const& found = target_.find_user_actions("user");
    // Only control privilege should remain
    EXPECT_TRUE(found.has_action(action_kind::control));
    EXPECT_TRUE(! found.has_action(action_kind::select));
    EXPECT_TRUE(! found.has_action(action_kind::insert));
    EXPECT_TRUE(! found.has_action(action_kind::update));
    EXPECT_TRUE(! found.has_action(action_kind::delete_));
}

TEST_F(fill_action_set_test, single_default_privilege) {
    // only one default privilege defined
    auto* priv = tdef_.add_default_privilege_list();
    priv->set_action_kind(proto::metadata::storage::TableActionKind::SELECT);

    action_set target{};
    from_default_privilege(tdef_, target);
    EXPECT_TRUE(target.has_action(action_kind::select));
    EXPECT_TRUE(! target.has_action(action_kind::insert));
    EXPECT_TRUE(! target.has_action(action_kind::update));
    EXPECT_TRUE(! target.has_action(action_kind::delete_));
}

TEST_F(fill_action_set_test, multiple_default_privileges) {
    auto* priv1 = tdef_.add_default_privilege_list();
    priv1->set_action_kind(proto::metadata::storage::TableActionKind::INSERT);
    auto* priv2 = tdef_.add_default_privilege_list();
    priv2->set_action_kind(proto::metadata::storage::TableActionKind::UPDATE);

    action_set target{};
    from_default_privilege(tdef_, target);
    EXPECT_TRUE(target.has_action(action_kind::insert));
    EXPECT_TRUE(target.has_action(action_kind::update));
    EXPECT_TRUE(! target.has_action(action_kind::select));
    EXPECT_TRUE(! target.has_action(action_kind::delete_));
}

TEST_F(fill_action_set_test, empty_default_privilege) {
    // No default privileges defined
    action_set target{};
    from_default_privilege(tdef_, target);
    EXPECT_TRUE(target.empty());
}

TEST_F(fill_action_set_test, from_action_sets) {
    // test from_action_sets function
    authorized_users_action_set users_actions{};
    users_actions.add_user_actions("userA", action_set{action_kind::select});
    users_actions.add_user_actions("userB", action_set{action_kind::insert});

    action_set public_actions{};
    public_actions.add_action(action_kind::update);
    public_actions.add_action(action_kind::delete_);

    proto::metadata::storage::TableDefinition target{};

    from_action_sets(users_actions, public_actions, target);

    authorized_users_action_set result_users{};
    from_authorization_list(target, result_users);

    auto const& foundA = result_users.find_user_actions("userA");
    EXPECT_TRUE(foundA.has_action(action_kind::select));
    EXPECT_TRUE(! foundA.has_action(action_kind::insert));
    EXPECT_TRUE(! foundA.has_action(action_kind::update));
    EXPECT_TRUE(! foundA.has_action(action_kind::delete_));

    auto const& foundB = result_users.find_user_actions("userB");
    EXPECT_TRUE(foundB.has_action(action_kind::insert));
    EXPECT_TRUE(! foundB.has_action(action_kind::select));
    EXPECT_TRUE(! foundB.has_action(action_kind::update));
    EXPECT_TRUE(! foundB.has_action(action_kind::delete_));

    action_set result_public{};
    from_default_privilege(target, result_public);
    EXPECT_TRUE(result_public.has_action(action_kind::update));
    EXPECT_TRUE(result_public.has_action(action_kind::delete_));
    EXPECT_TRUE(! result_public.has_action(action_kind::select));
    EXPECT_TRUE(! result_public.has_action(action_kind::insert));
}

}  // namespace jogasaki::auth
