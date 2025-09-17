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

#include <gtest/gtest.h>

#include <jogasaki/auth/action_kind.h>
#include <jogasaki/auth/action_set.h>

namespace jogasaki::auth {

class authorized_users_action_set_test : public ::testing::Test {
protected:
    authorized_users_action_set actions_{};
};

TEST_F(authorized_users_action_set_test, add_user_actions_and_find_user) {
    actions_.add_user_actions("user1", action_set{action_kind::select});

    auto const& found = actions_.find_user_actions("user1");
    EXPECT_TRUE(found.has_action(action_kind::select));
    EXPECT_TRUE(! found.has_action(action_kind::insert));
}

TEST_F(authorized_users_action_set_test, add_user_actions_empty_action_set_does_nothing) {
    action_set empty{};
    actions_.add_user_actions("user2", empty);
    auto const& found = actions_.find_user_actions("user2");
    EXPECT_TRUE(found.empty());
}

TEST_F(authorized_users_action_set_test, remove_user_all_actions) {
    actions_.add_user_actions("user3", action_set{action_kind::select});
    actions_.remove_user_all_actions("user3");
    auto const& found = actions_.find_user_actions("user3");
    EXPECT_TRUE(found.empty());
}

TEST_F(authorized_users_action_set_test, remove_user_action) {
    actions_.add_user_actions("user4", action_set{action_kind::select, action_kind::insert});

    actions_.remove_user_action("user4", action_kind::select);
    auto const& found = actions_.find_user_actions("user4");
    EXPECT_TRUE(! found.has_action(action_kind::select));
    EXPECT_TRUE(found.has_action(action_kind::insert));

    actions_.remove_user_action("user4", action_kind::insert);
    auto const& found2 = actions_.find_user_actions("user4");
    EXPECT_TRUE(found2.empty());
}

TEST_F(authorized_users_action_set_test, remove_user_actions) {
    actions_.add_user_actions("user4", action_set{action_kind::select, action_kind::insert});
    {
        auto const& found = actions_.find_user_actions("user4");
        EXPECT_TRUE(found.has_action(action_kind::select));
        EXPECT_TRUE(found.has_action(action_kind::insert));
    }
    actions_.remove_user_actions("user4", action_set{action_kind::select, action_kind::insert});
    {
        auto const& found = actions_.find_user_actions("user4");
        EXPECT_TRUE(! found.has_action(action_kind::select));
        EXPECT_TRUE(! found.has_action(action_kind::insert));
    }
}

TEST_F(authorized_users_action_set_test, remove_user_action_user_not_found) {
    actions_.remove_user_action("ghost", action_kind::select);
    // Should not throw or crash
}

TEST_F(authorized_users_action_set_test, remove_user_all_actions_user_not_found) {
    actions_.remove_user_all_actions("ghost");
    // Should not throw or crash
}

TEST_F(authorized_users_action_set_test, add_user_actions_merges_permissions) {
    // Grant select permission to user6
    actions_.add_user_actions("user6", action_set{action_kind::select});
    auto const& found1 = actions_.find_user_actions("user6");
    EXPECT_TRUE(found1.has_action(action_kind::select));
    EXPECT_TRUE(! found1.has_action(action_kind::insert));

    // Add insert permission to user6
    actions_.add_user_actions("user6", action_set{action_kind::insert});
    auto const& found2 = actions_.find_user_actions("user6");
    EXPECT_TRUE(found2.has_action(action_kind::select));
    EXPECT_TRUE(found2.has_action(action_kind::insert));
}

}  // namespace jogasaki::auth
