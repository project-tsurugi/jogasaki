/*
 * Copyright 2018-2020 tsurugi project.
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
#include <jogasaki/utils/storage_utils.h>

#include <gtest/gtest.h>

#include <takatori/type/int.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index_feature.h>

namespace jogasaki::utils {

using namespace yugawara;
using namespace testing;

class storage_utils_test : public ::testing::Test {};

TEST_F(storage_utils_test, simple) {
    storage::configurable_provider provider;
    namespace type = ::takatori::type;
    using ::yugawara::variable::nullity;
    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    yugawara::storage::index_feature_set secondary_index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
    };
    (void)secondary_index_features;
    {
        std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
            "T0",
            {
                { "C0", type::int8(), nullity{false} },
                { "C1", type::float8 (), nullity{true} },
            },
        });
        ASSERT_EQ(0, utils::index_count(*t));
        provider.add_index({
            t,
            t->simple_name(),
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
            },
            index_features
        });
        ASSERT_EQ(1, utils::index_count(*t));
        provider.add_index({
            t,
            "SECONDARY",
            {
                t->columns()[0],
            },
            {
                t->columns()[1],
            },
            secondary_index_features
        });
        ASSERT_EQ(2, utils::index_count(*t));
    }
}

}

