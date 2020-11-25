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
#include <jogasaki/executor/process/mock/cogroup.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/impl/ops/join.h>

#include <jogasaki/executor/process/mock/group_reader.h>

namespace jogasaki::executor::process::mock {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class cogroup_test : public test_root {

public:
    using kind = field_type_kind;

};

using group_type = mock::group_reader::group_type;
using keys_type = group_type::key_type;
using values_type = group_type::value_type;

TEST_F(cogroup_test, simple) {
    mock::group_reader r1 {
        {
            group_type{
                keys_type{1},
                {
                    values_type{100.0},
                    values_type{101.0},
                },
            },
            group_type{
                keys_type{2},
                {
                    values_type{200.0},
                },
            },
        }
    };
    mock::group_reader r2 {
        {
            group_type{
                keys_type{1},
                {
                    values_type{100.0},
                    values_type{101.0},
                },
            },
            group_type{
                keys_type{3},
                {
                    values_type{300.0},
                },
            },
        }
    };

    auto meta = test_group_meta1();
    auto key_offset = meta->key().value_offset(0);
    auto value_offset = meta->value().value_offset(0);

    cogroup cgrp{
        std::vector<executor::group_reader*>{&r1, &r2},
        std::vector<maybe_shared_ptr<meta::group_meta>>{test_group_meta1(), test_group_meta1()}
    };

    using consumer_type = std::function<void(accessor::record_ref, std::vector<cogroup::iterator_pair>&)>;
    std::vector<std::int64_t> keys{};
    std::vector<std::vector<double>> values1{};
    std::vector<std::vector<double>> values2{};
    consumer_type consumer = [&](accessor::record_ref key, std::vector<cogroup::iterator_pair>& values) {
        keys.emplace_back(key.get_value<std::int64_t>(key_offset));
        auto& r1 = values1.emplace_back();
        auto& r2 = values2.emplace_back();
        for(auto b = values[0].first; b != values[0].second; ++b) {
            r1.emplace_back((*b).get_value<double>(value_offset));
        }
        for(auto b = values[1].first; b != values[1].second; ++b) {
            r2.emplace_back((*b).get_value<double>(value_offset));
        }
    };
    cgrp(consumer);

    auto exp = std::vector<std::int64_t>{1,2,3};
    auto v1 = std::vector<std::vector<double>>{
            {100.0, 101.0},
            {200.0},
            {}
        };
    auto v2 = std::vector<std::vector<double>>{
            {100.0, 101.0},
            {},
            {300.0}
    };
    EXPECT_EQ(exp, keys);
    EXPECT_EQ(v1, values1);
    EXPECT_EQ(v2, values2);

}

TEST_F(cogroup_test, three_inputs) {
    mock::group_reader r1 {
        {
            group_type{
                keys_type{1},
                {
                    values_type{100.0},
                },
            },
            group_type{
                keys_type{2},
                {
                    values_type{200.0},
                    values_type{201.0},
                },
            },
        }
    };
    mock::group_reader r2 {
        {
            group_type{
                keys_type{1},
                {
                    values_type{101.0},
                },
            },
            group_type{
                keys_type{2},
                {
                    values_type{200.0},
                },
            },
            group_type{
                keys_type{3},
                {
                    values_type{300.0},
                },
            },
        }
    };
    mock::group_reader r3 {
        {
            group_type{
                keys_type{3},
                {
                    values_type{301.0},
                },
            },
        }
    };

    auto meta = test_group_meta1();
    auto key_offset = meta->key().value_offset(0);
    auto value_offset = meta->value().value_offset(0);

    cogroup cgrp{
            std::vector<executor::group_reader*>{&r1, &r2, &r3},
            std::vector<maybe_shared_ptr<meta::group_meta>>{test_group_meta1(), test_group_meta1(), test_group_meta1()}
    };

    using consumer_type = std::function<void(accessor::record_ref, std::vector<cogroup::iterator_pair>&)>;
    std::vector<std::int64_t> keys{};
    std::vector<std::vector<double>> values1{};
    std::vector<std::vector<double>> values2{};
    std::vector<std::vector<double>> values3{};
    consumer_type consumer = [&](accessor::record_ref key, std::vector<cogroup::iterator_pair>& values) {
        keys.emplace_back(key.get_value<std::int64_t>(key_offset));
        auto& r1 = values1.emplace_back();
        auto& r2 = values2.emplace_back();
        auto& r3 = values3.emplace_back();
        for(auto b = values[0].first; b != values[0].second; ++b) {
            r1.emplace_back((*b).get_value<double>(value_offset));
        }
        for(auto b = values[1].first; b != values[1].second; ++b) {
            r2.emplace_back((*b).get_value<double>(value_offset));
        }
        for(auto b = values[2].first; b != values[2].second; ++b) {
            r3.emplace_back((*b).get_value<double>(value_offset));
        }
    };
    cgrp(consumer);

    auto exp = std::vector<std::int64_t>{1,2,3};
    auto v1 = std::vector<std::vector<double>>{
            {100.0},
            {200.0, 201.0},
            {}
    };
    auto v2 = std::vector<std::vector<double>>{
            {101.0},
            {200.0},
            {300.0}
    };
    auto v3 = std::vector<std::vector<double>>{
            {},
            {},
            {301.0}
    };
    EXPECT_EQ(exp, keys);
    EXPECT_EQ(v1, values1);
    EXPECT_EQ(v2, values2);
    EXPECT_EQ(v3, values3);
}

TEST_F(cogroup_test, key_value_reversed) {
    mock::group_reader r1 {
        {
            group_type{
                keys_type{1},
                {
                    values_type{100.0},
                    values_type{101.0},
                },
            },
            group_type{
                keys_type{2},
                {
                    values_type{200.0},
                },
            },
        }
    };
    mock::group_reader r2 {
        {
            group_type{
                keys_type{1},
                {
                    values_type{100.0},
                    values_type{101.0},
                },
            },
            group_type{
                keys_type{3},
                {
                    values_type{300.0},
                },
            },
        }
    };

    auto meta = test_group_meta1_kv_reversed();
    auto value_offset = meta->value().value_offset(0);
    auto key_offset = meta->key().value_offset(0);

    cogroup cgrp{
            std::vector<executor::group_reader*>{&r1, &r2},
            std::vector<maybe_shared_ptr<meta::group_meta>>{test_group_meta1_kv_reversed(), test_group_meta1_kv_reversed()}
    };

    using consumer_type = std::function<void(accessor::record_ref, std::vector<cogroup::iterator_pair>&)>;
    std::vector<std::int64_t> keys{};
    std::vector<std::vector<double>> values1{};
    std::vector<std::vector<double>> values2{};
    consumer_type consumer = [&](accessor::record_ref key, std::vector<cogroup::iterator_pair>& values) {
        keys.emplace_back(key.get_value<std::int64_t>(key_offset));
        auto& r1 = values1.emplace_back();
        auto& r2 = values2.emplace_back();
        for(auto b = values[0].first; b != values[0].second; ++b) {
            r1.emplace_back((*b).get_value<double>(value_offset));
        }
        for(auto b = values[1].first; b != values[1].second; ++b) {
            r2.emplace_back((*b).get_value<double>(value_offset));
        }
    };
    cgrp(consumer);

    auto exp = std::vector<std::int64_t>{1,2,3};
    auto v1 = std::vector<std::vector<double>>{
            {100.0, 101.0},
            {200.0},
            {}
    };
    auto v2 = std::vector<std::vector<double>>{
            {100.0, 101.0},
            {},
            {300.0}
    };
    EXPECT_EQ(exp, keys);
    EXPECT_EQ(v1, values1);
    EXPECT_EQ(v2, values2);

}

}

