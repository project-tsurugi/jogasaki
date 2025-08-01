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
#include <gtest/gtest.h>

#include <jogasaki/auth/action_set.h>

namespace jogasaki::auth {

class action_set_test : public ::testing::Test {

};

TEST(action_set_test, empty) {
    action_set s{};
    EXPECT_TRUE(s.empty());
    s.add_action(action_kind::select);
    EXPECT_TRUE(! s.empty());
}

TEST(action_set_test, clear) {
    action_set s{};
    s.add_action(action_kind::select);
    EXPECT_TRUE(! s.empty());
    s.clear();
    EXPECT_TRUE(s.empty());
}

TEST(action_set_test, has_action) {
    action_set s{};
    EXPECT_TRUE(! s.has_action(action_kind::select));
    EXPECT_TRUE(! s.has_action(action_kind::insert));
    EXPECT_TRUE(! s.has_action(action_kind::update));
    EXPECT_TRUE(! s.has_action(action_kind::delete_));
    EXPECT_TRUE(! s.has_action(action_kind::control));

    s.add_action(action_kind::select);
    EXPECT_TRUE(s.has_action(action_kind::select));
    EXPECT_TRUE(! s.has_action(action_kind::insert));
    EXPECT_TRUE(! s.has_action(action_kind::update));
    EXPECT_TRUE(! s.has_action(action_kind::delete_));
    EXPECT_TRUE(! s.has_action(action_kind::control));
}

TEST(action_set_test, add) {
    action_set s{};
    s.add_action(action_kind::select);
    EXPECT_TRUE(s.action_allowed(action_kind::select));
    EXPECT_TRUE(s.has_action(action_kind::select));
    EXPECT_TRUE(! s.action_allowed(action_kind::insert));
    EXPECT_TRUE(! s.has_action(action_kind::insert));
    ASSERT_NO_THROW(s.add_action(action_kind::select));
}

TEST(action_set_test, remove) {
    action_set s{};
    s.add_action(action_kind::select);
    EXPECT_TRUE(s.action_allowed(action_kind::select));
    s.remove_action(action_kind::select);
    EXPECT_TRUE(! s.has_action(action_kind::select));
    EXPECT_TRUE(! s.action_allowed(action_kind::select));
    ASSERT_NO_THROW(s.remove_action(action_kind::select));
    EXPECT_TRUE(! s.has_action(action_kind::select));
    EXPECT_TRUE(! s.action_allowed(action_kind::select));
}

TEST(action_set_test, control) {
    // control is special case in that it implies all actions and adding it deletes any others
    action_set s{};
    s.add_action(action_kind::control);
    EXPECT_TRUE(! s.empty());
    EXPECT_TRUE(s.has_action(action_kind::control));
    EXPECT_TRUE(! s.has_action(action_kind::select));
    EXPECT_TRUE(! s.has_action(action_kind::insert));
    EXPECT_TRUE(! s.has_action(action_kind::update));
    EXPECT_TRUE(! s.has_action(action_kind::delete_));

    EXPECT_TRUE(s.action_allowed(action_kind::control));
    EXPECT_TRUE(s.action_allowed(action_kind::select));
    EXPECT_TRUE(s.action_allowed(action_kind::insert));
    EXPECT_TRUE(s.action_allowed(action_kind::update));
    EXPECT_TRUE(s.action_allowed(action_kind::delete_));

    ASSERT_NO_THROW(s.remove_action(action_kind::select)); // removing select does nothing
    EXPECT_TRUE(s.action_allowed(action_kind::select));
    EXPECT_TRUE(! s.has_action(action_kind::select));

    s.remove_action(action_kind::control);
    EXPECT_TRUE(! s.has_action(action_kind::control));
    EXPECT_TRUE(! s.has_action(action_kind::select));
    EXPECT_TRUE(! s.has_action(action_kind::insert));
    EXPECT_TRUE(! s.has_action(action_kind::update));
    EXPECT_TRUE(! s.has_action(action_kind::delete_));
    EXPECT_TRUE(! s.action_allowed(action_kind::control));
    EXPECT_TRUE(! s.action_allowed(action_kind::select));
    EXPECT_TRUE(! s.action_allowed(action_kind::insert));
    EXPECT_TRUE(! s.action_allowed(action_kind::update));
    EXPECT_TRUE(! s.action_allowed(action_kind::delete_));
}

TEST(action_set_test, allows) {
    action_set s{};
    s.add_action(action_kind::select);
    s.add_action(action_kind::insert);

    action_set t{};
    t.add_action(action_kind::select);
    EXPECT_TRUE(s.allows(t));

    t.add_action(action_kind::insert);
    EXPECT_TRUE(s.allows(t));

    t.add_action(action_kind::update);
    EXPECT_TRUE(! s.allows(t));

    s.add_action(action_kind::control);
    EXPECT_TRUE(s.allows(t));

    t.add_action(action_kind::control);
    EXPECT_TRUE(s.allows(t));
}

}  // namespace jogasaki::auth
