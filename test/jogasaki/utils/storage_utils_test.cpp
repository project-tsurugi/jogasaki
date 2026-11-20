/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <memory>
#include <gtest/gtest.h>

#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/utils/storage_utils.h>
#include <jogasaki/executor/wrt/fill_record_fields.h>
#include <jogasaki/index/utils.h>


namespace jogasaki::utils {

using namespace yugawara;
using namespace testing;
namespace storage = ::yugawara::storage;

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

// Fail on provider queries from an enumeration callback, without relying on
// the platform's recursive shared_mutex behavior or a timing-based deadlock.
class observed_storage_provider : public storage::configurable_provider {
public:
    mutable std::size_t enumerations{};
    mutable std::size_t primary_lookups{};
    mutable bool in_callback{};

    void each_index(index_consumer_type const& consumer) const override {
        ++enumerations;
        storage::configurable_provider::each_index([&](auto name, auto const& entry) {
            in_callback = true;
            consumer(name, entry);
            in_callback = false;
        });
    }

    std::shared_ptr<storage::index const> find_primary_index(storage::table const& table) const override {
        ++primary_lookups;
        EXPECT_FALSE(in_callback);
        return storage::configurable_provider::find_primary_index(table);
    }
};

TEST_F(storage_utils_test, collects_secondary_indices_and_prepares_outside_provider) {
    observed_storage_provider provider{};
    auto table = provider.add_table(storage::table{"T", {
        {"K", ::takatori::type::int8{}, variable::nullity{false}},
        {"V", ::takatori::type::int8{}, variable::nullity{true}},
    }});
    auto primary = provider.add_index(storage::index{table, "T", {table->columns()[0]},
        {table->columns()[1]}, {storage::index_feature::primary}});
    auto secondary = provider.add_index(storage::index{table, "S", {table->columns()[1]}, {}, {}});
    auto other = provider.add_table(storage::table{"OTHER", {{"K", ::takatori::type::int8{}}}});
    provider.add_index(storage::index{other, "UNRELATED", {other->columns()[0]}, {}, {}});

    auto collected = collect_secondary_indices(*primary);
    ASSERT_EQ(1, collected.size());
    EXPECT_EQ(secondary, collected.front());
    EXPECT_EQ(1, provider.enumerations);
    provider.enumerations = 0;
    auto targets = executor::wrt::create_secondary_targets(*primary,
        index::create_meta(*primary, true), index::create_meta(*primary, false));
    ASSERT_EQ(1, targets.size());
    EXPECT_EQ("S", targets.front().storage_name());
    EXPECT_EQ(1, provider.enumerations);
    EXPECT_EQ(0, provider.primary_lookups);
    EXPECT_FALSE(provider.in_callback);

    // The collected reference survives removal; targets own their storage name.
    EXPECT_TRUE(provider.remove_index("S"));
    secondary.reset();
    EXPECT_EQ("S", collected.front()->simple_name());
    EXPECT_EQ("S", targets.front().storage_name());
}

TEST_F(storage_utils_test, collects_no_secondaries_for_primary_only_table) {
    storage::configurable_provider provider{};
    auto table = provider.add_table(storage::table{"T", {{"K", ::takatori::type::int8{}}}});
    auto primary = provider.add_index(storage::index{table, "T", {table->columns()[0]}, {},
        {storage::index_feature::primary}});
    EXPECT_TRUE(collect_secondary_indices(*primary).empty());
    EXPECT_TRUE(executor::wrt::create_secondary_targets(*primary,
        index::create_meta(*primary, true), index::create_meta(*primary, false)).empty());
}

}

