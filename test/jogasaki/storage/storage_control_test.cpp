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
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/writer_count_calculator.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::storage {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

namespace type      = ::takatori::type;
namespace value     = ::takatori::value;
namespace scalar    = ::takatori::scalar;
namespace relation  = ::takatori::relation;
namespace statement = ::takatori::statement;

using namespace testing;

using namespace ::yugawara::variable;
using namespace ::yugawara;

/**
 * @brief test storage control object
 */
class storage_control_test : public ::testing::Test {

};

TEST_F(storage_control_test, storage_control_unique) {
    impl::storage_control ctrl{};
    EXPECT_TRUE(ctrl.can_lock());
    EXPECT_TRUE(ctrl.lock());
    EXPECT_TRUE(! ctrl.can_lock());
    EXPECT_TRUE(! ctrl.lock());
    ctrl.release();
    EXPECT_TRUE(ctrl.can_lock());
    EXPECT_TRUE(ctrl.lock());
    EXPECT_TRUE(! ctrl.lock());
    ctrl.release();
    EXPECT_TRUE(ctrl.can_lock());
    EXPECT_TRUE(ctrl.lock());
    ctrl.release();
    ASSERT_THROW(ctrl.release(), std::logic_error);
}

TEST_F(storage_control_test, storage_control_release_error) {
    impl::storage_control ctrl{};
    ASSERT_THROW(ctrl.release(), std::logic_error);
}

TEST_F(storage_control_test, storage_control_release_shared_error) {
    impl::storage_control ctrl{};
    ASSERT_THROW(ctrl.release_shared(), std::logic_error);
}

TEST_F(storage_control_test, storage_control_shared) {
    impl::storage_control ctrl{};
    EXPECT_TRUE(ctrl.can_lock_shared());
    EXPECT_TRUE(ctrl.lock_shared());
    ctrl.release_shared();
    EXPECT_TRUE(ctrl.can_lock_shared());
    EXPECT_TRUE(ctrl.lock_shared());
    EXPECT_TRUE(ctrl.can_lock_shared());
    EXPECT_TRUE(ctrl.lock_shared());
    EXPECT_TRUE(ctrl.can_lock_shared());
    ctrl.release_shared();
    EXPECT_TRUE(ctrl.can_lock_shared());
    ctrl.release_shared();
    EXPECT_TRUE(ctrl.can_lock_shared());
    ASSERT_THROW(ctrl.release_shared(), std::logic_error);
}

TEST_F(storage_control_test, storage_control_mixed) {
    impl::storage_control ctrl{};
    EXPECT_TRUE(ctrl.can_lock_shared());
    EXPECT_TRUE(ctrl.lock_shared());
    EXPECT_TRUE(! ctrl.can_lock());
    EXPECT_TRUE(! ctrl.lock());
    ctrl.release_shared();
    EXPECT_TRUE(ctrl.can_lock());
    EXPECT_TRUE(ctrl.lock());
    EXPECT_TRUE(! ctrl.can_lock_shared());
    EXPECT_TRUE(! ctrl.lock_shared());
    ctrl.release();
    EXPECT_TRUE(ctrl.can_lock_shared());
    EXPECT_TRUE(ctrl.can_lock());
}

} //
