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
#include <jogasaki/storage/storage_operation.h>

#include <gtest/gtest.h>

#include <jogasaki/auth/action_kind.h>
#include <jogasaki/auth/action_set.h>

namespace jogasaki::storage {

class storage_operation_test : public ::testing::Test {};

TEST_F(storage_operation_test, storage_list_and_action_set_pairing) {
    // Prepare storage entries and action sets
    storage_entry e1 = 1;
    storage_entry e2 = 2;
    storage_entry e3 = 3;
    storage_list list{{e1, e2, e3}};
    std::vector<auth::action_set> actions(3);

    // Set some actions for test
    actions[0].add_action(auth::action_kind::select);
    actions[1].add_action(auth::action_kind::insert);
    actions[2].add_action(auth::action_kind::update);

    storage_operation op{list, actions};

    // Check storage_list_view
    storage_list_view view = op.storage();
    EXPECT_EQ(view.size(), 3U);
    EXPECT_TRUE(view.contains(e1));
    EXPECT_TRUE(view.contains(e2));
    EXPECT_TRUE(view.contains(e3));

    // Check iterator
    std::vector<storage_entry> found_entries;
    std::vector<auth::action_set> found_actions;
    for (auto&& [entry, action_set] : op) {
        found_entries.emplace_back(entry);
        found_actions.emplace_back(std::move(action_set));
    }
    EXPECT_EQ(found_entries.size(), 3U);
    EXPECT_EQ(found_actions.size(), 3U);
    EXPECT_EQ(list, storage_list{found_entries});
    EXPECT_EQ(actions, found_actions);
}

}  // namespace jogasaki::storage
