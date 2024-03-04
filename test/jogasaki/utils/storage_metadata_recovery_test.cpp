/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <jogasaki/recovery/storage_options.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>
#include <takatori/value/date.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>
#include <takatori/value/decimal.h>
#include <takatori/value/octet.h>
#include <takatori/util/string_builder.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/utils/storage_metadata_exception.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/utils/proto_debug_string.h>

#include <gtest/gtest.h>

namespace jogasaki::recovery {

using namespace yugawara;
namespace type = ::takatori::type;
using ::yugawara::variable::nullity;
using ::yugawara::storage::column_value;

class storage_metadata_recovery_test : public ::testing::Test {
public:
    yugawara::storage::index_feature_set index_features_{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    yugawara::storage::index_feature_set secondary_index_features_{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
    };
};

TEST_F(storage_metadata_recovery_test, merge_providers_simple) {
    auto src = std::make_shared<storage::configurable_provider>();
    auto dest = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<::yugawara::storage::table> t = src->add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
        },
    });
    auto primary = src->add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {},
        index_features_
    });
    ASSERT_FALSE(merge_deserialized_storage_option(*src, *dest, false));
    auto found_table = dest->find_table("TT");
    EXPECT_TRUE(found_table);
    auto found_index = dest->find_index("TT");
    EXPECT_TRUE(found_index);
    EXPECT_FALSE(src->find_table("TT"));
    EXPECT_FALSE(src->find_index("TT"));
}

TEST_F(storage_metadata_recovery_test, merge_providers_hit_already_exists) {
    auto src = std::make_shared<storage::configurable_provider>();
    auto dest = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<::yugawara::storage::table> t = src->add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
        },
    });
    auto primary = src->add_index({
        t,
        t->simple_name(),
        {
            t->columns()[0],
        },
        {},
        index_features_
    });
    ASSERT_FALSE(merge_deserialized_storage_option(*src, *dest, false));
    auto dest2 = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<::yugawara::storage::table> t2 = dest2->add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
        },
    });
    auto primary2 = dest2->add_index({
        t2,
        t2->simple_name(),
        {
            t2->columns()[0],
        },
        {},
        index_features_
    });
    auto err = merge_deserialized_storage_option(*dest, *dest2, false);
    ASSERT_TRUE(err);
    EXPECT_EQ(status::err_already_exists, err->status());
    EXPECT_EQ(error_code::target_already_exists_exception, err->code());
    EXPECT_EQ("table \"TT\" already exists", std::string{err->message()});
}
}
