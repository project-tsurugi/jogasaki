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
#include <jogasaki/executor/process/cogroup.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include <jogasaki/executor/process/join.h>

#include "mock/group_reader.h"

namespace jogasaki::executor::process {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class cogroup_test : public test_root {

public:
    using kind = field_type_kind;

};

TEST_F(cogroup_test, simple) {
    mock::group_reader r1 {
            {
                    mock::group_entry{
                            1,
                            mock::group_entry::value_type{
                                    100.0,
                                    101.0,
                            },
                    },
                    mock::group_entry{
                            2,
                            mock::group_entry::value_type{
                                    200.0,
                            },
                    },
            }
    };
    mock::group_reader r2 {
            {
                    mock::group_entry{
                            1,
                            mock::group_entry::value_type{
                                    100.0,
                                    101.0,
                            },
                    },
                    mock::group_entry{
                            3,
                            mock::group_entry::value_type{
                                    300.0,
                            },
                    },
            }
    };

    auto meta = test_group_meta1();
    auto value_offset = meta->value().value_offset(0);

    cogroup cgrp{
        std::vector<group_reader*>{&r1, &r2},
        std::vector<std::shared_ptr<meta::group_meta>>{test_group_meta1(), test_group_meta1()}
    };

    using consumer_type = std::function<void(accessor::record_ref, std::vector<impl::iterator_pair>&)>;
    std::vector<std::int64_t> keys{};
    std::vector<std::vector<double>> values1{};
    std::vector<std::vector<double>> values2{};
    consumer_type consumer = [&](accessor::record_ref key, std::vector<impl::iterator_pair>& values) {
        keys.emplace_back(key.get_value<std::int64_t>(0));
        auto& r1 = values1.emplace_back();
        auto& r2 = values2.emplace_back();
        for(auto b = values[0].first; b != values[0].second; ++b) {
            auto rec = accessor::record_ref((*b), meta->value().record_size());
            r1.emplace_back(rec.get_value<double>(value_offset));
        }
        for(auto b = values[1].first; b != values[1].second; ++b) {
            auto rec = accessor::record_ref((*b), meta->value().record_size());
            r2.emplace_back(rec.get_value<double>(value_offset));
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
                    mock::group_entry{
                            1,
                            mock::group_entry::value_type{
                                    100.0,
                            },
                    },
                    mock::group_entry{
                            2,
                            mock::group_entry::value_type{
                                    200.0,
                                    201.0,
                            },
                    },
            }
    };
    mock::group_reader r2 {
            {
                    mock::group_entry{
                            1,
                            mock::group_entry::value_type{
                                    101.0,
                            },
                    },
                    mock::group_entry{
                            2,
                            mock::group_entry::value_type{
                                    200.0,
                            },
                    },
                    mock::group_entry{
                            3,
                            mock::group_entry::value_type{
                                    300.0,
                            },
                    },
            }
    };
    mock::group_reader r3 {
            {
                    mock::group_entry{
                            3,
                            mock::group_entry::value_type{
                                    301.0,
                            },
                    },
            }
    };

    auto meta = test_group_meta1();
    auto value_offset = meta->value().value_offset(0);

    cogroup cgrp{
            std::vector<group_reader*>{&r1, &r2, &r3},
            std::vector<std::shared_ptr<meta::group_meta>>{test_group_meta1(), test_group_meta1(), test_group_meta1()}
    };

    using consumer_type = std::function<void(accessor::record_ref, std::vector<impl::iterator_pair>&)>;
    std::vector<std::int64_t> keys{};
    std::vector<std::vector<double>> values1{};
    std::vector<std::vector<double>> values2{};
    std::vector<std::vector<double>> values3{};
    consumer_type consumer = [&](accessor::record_ref key, std::vector<impl::iterator_pair>& values) {
        keys.emplace_back(key.get_value<std::int64_t>(0));
        auto& r1 = values1.emplace_back();
        auto& r2 = values2.emplace_back();
        auto& r3 = values3.emplace_back();
        for(auto b = values[0].first; b != values[0].second; ++b) {
            auto rec = accessor::record_ref((*b), meta->value().record_size());
            r1.emplace_back(rec.get_value<double>(value_offset));
        }
        for(auto b = values[1].first; b != values[1].second; ++b) {
            auto rec = accessor::record_ref((*b), meta->value().record_size());
            r2.emplace_back(rec.get_value<double>(value_offset));
        }
        for(auto b = values[2].first; b != values[2].second; ++b) {
            auto rec = accessor::record_ref((*b), meta->value().record_size());
            r3.emplace_back(rec.get_value<double>(value_offset));
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

}

