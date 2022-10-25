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
#include <jogasaki/utils/storage_metadata_serializer.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/decimal.h>
#include <takatori/datetime/time_zone.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/proto/metadata/storage.pb.h>

#include <gtest/gtest.h>

namespace jogasaki::utils {

using namespace yugawara;
namespace type = ::takatori::type;
using ::yugawara::variable::nullity;

class storage_metadata_serializer_test : public ::testing::Test {};

std::string readable(std::string_view serialized) {
    proto::metadata::storage::IndexDefinition def{};
    if (!def.ParseFromArray(serialized.data(), serialized.size())) {
        []() {
            FAIL();
        }();
    }
    return def.Utf8DebugString();
}

TEST_F(storage_metadata_serializer_test, simple) {
    storage::configurable_provider provider{};

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

    std::shared_ptr<::yugawara::storage::table> t = provider.add_table({
        "TT",
        {
            { "C0", type::int8(), nullity{false} },
            { "C1", type::character(type::varying), nullity{true} },
        },
    });
    auto primary = provider.add_index({
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

    auto secondary = provider.add_index({
        t,
        "TT_SECONDARY",
        {
            t->columns()[1],
        },
        {},
        secondary_index_features
    });

    storage_metadata_serializer ser{};
    std::string out{};
    ASSERT_TRUE(ser.serialize_primary_index(*primary, out));

    std::shared_ptr<storage::configurable_provider> deserialized{};
    ASSERT_TRUE(ser.deserialize(out, provider, deserialized));
    auto i = deserialized->find_index("TT");
    ASSERT_TRUE(i);
    std::string out2{};
    ASSERT_TRUE(ser.serialize_primary_index(*i, out2));
    EXPECT_EQ(readable(out2), readable(out));
}

}

