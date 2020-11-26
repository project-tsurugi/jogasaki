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

#include <jogasaki/executor/process/mock/group_reader.h>

namespace jogasaki::executor::process::mock {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace jogasaki::mock;

class cogroup_test : public test_root {

public:
    using kind = field_type_kind;

    basic_record create_key(
        std::int64_t arg0
    ) {
        return create_record<kind::int8>(arg0);
    }

    basic_record create_value(
        double arg0
    ) {
        return create_record<kind::float8>(arg0);
    }
};

using group_type = mock::basic_group_reader::group_type;
using keys_type = group_type::key_type;
using value_type = group_type::value_type;

TEST_F(cogroup_test, simple) {
    auto meta = test_group_meta1();
    mock::basic_group_reader r1 {
        {
            group_type{
                create_key(1),
                {
                    create_value(100.0),
                    create_value(101.0),
                },
            },
            group_type{
                create_key(2),
                {
                    create_value(200.0),
                },
            },
        },
        meta
    };
    mock::basic_group_reader r2 {
        {
            group_type{
                create_key(1),
                {
                    create_value(100.0),
                    create_value(101.0),
                },
            },
            group_type{
                create_key(3),
                {
                    create_value(300.0),
                },
            },
        },
        meta
    };

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
    auto meta = test_group_meta1();
    mock::basic_group_reader r1 {
        {
            group_type{
                create_key(1),
                {
                    create_value(100.0),
                },
            },
            group_type{
                create_key(2),
                {
                    create_value(200.0),
                    create_value(201.0),
                },
            },
        },
        meta
    };
    mock::basic_group_reader r2 {
        {
            group_type{
                create_key(1),
                {
                    create_value(101.0),
                },
            },
            group_type{
                create_key(2),
                {
                    create_value(200.0),
                },
            },
            group_type{
                create_key(3),
                {
                    create_value(300.0),
                },
            },
        },
        meta
    };
    mock::basic_group_reader r3 {
        {
            group_type{
                create_key(3),
                {
                    create_value(301.0),
                },
            },
        },
        meta
    };

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
    auto meta = test_group_meta1_kv_reversed();
    mock::basic_group_reader r1 {
        {
            group_type{
                create_key(1),
                {
                    create_value(100.0),
                    create_value(101.0),
                },
            },
            group_type{
                create_key(2),
                {
                    create_value(200.0),
                },
            },
        },
        meta
    };
    mock::basic_group_reader r2 {
        {
            group_type{
                create_key(1),
                {
                    create_value(100.0),
                    create_value(101.0),
                },
            },
            group_type{
                create_key(3),
                {
                    create_value(300.0),
                },
            },
        },
        meta
    };

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

