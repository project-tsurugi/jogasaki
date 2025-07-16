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

#include <cstdint>
#include <iterator>
#include <string>
#include <string_view>
#include <type_traits>
#include <boost/container/container_fwd.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/constants.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/group/input_partition.h>
#include <jogasaki/executor/exchange/shuffle/pointer_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/record.h>

namespace jogasaki::executor::exchange::group {

using namespace testing;
using namespace data;
using namespace executor;
using namespace meta;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class input_partition_test : public test_root {
public:
};

using kind = meta::field_type_kind;

TEST_F(input_partition_test, basic) {
    auto context = std::make_shared<request_context>();
    input_partition partition{
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_shared<group_info>(test_record_meta1(), std::vector<std::size_t>{0}), context.get()};
    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
}

TEST_F(input_partition_test, use_monotonic_resource) {
    memory::page_pool pool{};
    auto context = std::make_shared<request_context>();
    input_partition partition{
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_shared<group_info>(test_record_meta1(), std::vector<std::size_t>{0}),
            context.get(),
            };

    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();

    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
}

TEST_F(input_partition_test, auto_flush_to_next_table_when_full) {
    auto context = std::make_shared<request_context>();
    auto meta = test_record_meta1();
    input_partition partition{
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_shared<group_info>(meta, std::vector<std::size_t>{0}),
            context.get(),
            2
            };
    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();

    auto record_size = meta->record_size();
    auto c1_offset = meta->value_offset(0);
    auto c2_offset = meta->value_offset(1);
    ASSERT_EQ(2, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t0 = *partition.begin();
    EXPECT_EQ(2, std::distance(t0.begin(), t0.end()));
    auto it = t0.begin();
    EXPECT_EQ(1, accessor::record_ref(*it, record_size).get_value<std::int64_t>(c1_offset));
    EXPECT_EQ(3, accessor::record_ref(*++it, record_size).get_value<std::int64_t>(c1_offset));
    auto& t1 = *++partition.begin();
    EXPECT_EQ(1, std::distance(t1.begin(), t1.end()));
    EXPECT_EQ(2, accessor::record_ref(*t1.begin(), record_size).get_value<std::int64_t>(c1_offset));
}

TEST_F(input_partition_test, text) {
    auto context = std::make_shared<request_context>();
    struct S {
        text t1_{};
        double f_{};
        text t2_{};
    };
    auto meta = std::make_shared<meta::record_meta>(
        std::vector<field_type>{
            field_type(std::make_shared<meta::character_field_option>()),
            field_type(field_enum_tag<kind::float8>),
            field_type(std::make_shared<meta::character_field_option>()),
        },
        boost::dynamic_bitset<std::uint64_t>{"000"s},
        std::vector<std::size_t>{
            offsetof(S, t1_),
            offsetof(S, f_),
            offsetof(S, t2_),
        },
        std::vector<std::size_t>{0, 0, 0},
        alignof(S),
        sizeof(S)
    );
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_shared<group_info>(
            meta,
            std::vector<std::size_t>{0}),
        context.get(),
    };

    mock_memory_resource res{};
    S r1{text{&res, "111"sv}, 1.0, text{&res, "AAA"}};
    S r2{text{&res, "222"sv}, 2.0, text{&res, "BBB"}};
    S r3{text{&res, "333"sv}, 3.0, text{&res, "CCC"}};
    accessor::record_ref ref1{&r1, sizeof(S)};
    accessor::record_ref ref2{&r2, sizeof(S)};
    accessor::record_ref ref3{&r3, sizeof(S)};

    partition.write(ref3);
    partition.write(ref1);
    partition.write(ref2);
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    ASSERT_EQ(3, std::distance(t.begin(), t.end()));
    auto it = t.begin();
    accessor::record_ref res1{*it++, sizeof(S)};
    accessor::record_ref res2{*it++, sizeof(S)};
    accessor::record_ref res3{*it++, sizeof(S)};

    compare_info cm{*meta};
    comparator comp{cm};
    EXPECT_EQ(0, comp(ref1, res1));
    EXPECT_EQ(0, comp(ref2, res2));
    EXPECT_EQ(0, comp(ref3, res3));
}

TEST_F(input_partition_test, empty_keys) {
    auto context = std::make_shared<request_context>();
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_shared<group_info>(
            test_record_meta1(),
            std::vector<std::size_t>{},
        std::vector<std::size_t>{},
        std::vector<ordering>{}
        ), context.get()};
    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
}

TEST_F(input_partition_test, sort_keys_only) {
    auto context = std::make_shared<request_context>();
    auto meta = test_record_meta1();
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_shared<group_info>(
            meta,
            std::vector<std::size_t>{},
            std::vector<std::size_t>{0, 1},
            std::vector<ordering>{ordering::ascending, ordering::descending}
        ), context.get()};
    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
    auto it = t.begin();
    mock::basic_record res0{accessor::record_ref{*it++, meta->record_size()}, meta};
    mock::basic_record res1{accessor::record_ref{*it++, meta->record_size()}, meta};
    mock::basic_record res2{accessor::record_ref{*it++, meta->record_size()}, meta};
    EXPECT_EQ(r1, res0);
    EXPECT_EQ(r2, res1);
    EXPECT_EQ(r3, res2);
}

TEST_F(input_partition_test, sort_asc) {
    auto context = std::make_shared<request_context>();
    struct S {
        std::int64_t i1_{};
        std::int64_t i2_{};
        std::int64_t i3_{};
        std::int64_t i4_{};
        char n_[1];
    };
    std::size_t nullity_base = offsetof(S, n_) * bits_per_byte;
    auto meta = std::make_shared<meta::record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{4}.flip(),
        std::vector<std::size_t>{
            offsetof(S, i1_),
            offsetof(S, i2_),
            offsetof(S, i3_),
            offsetof(S, i4_),
        },
        std::vector<std::size_t>{
            nullity_base + 0,
            nullity_base + 1,
            nullity_base + 2,
            nullity_base + 3,
        },
        alignof(S),
        sizeof(S)
    );
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_shared<group_info>(
            meta,
            std::vector<std::size_t>{0},
            std::vector<std::size_t>{1, 2},
            std::vector<ordering>{ordering::ascending, ordering::ascending}
        ),
        context.get(),
    };

    mock_memory_resource res{};
    S r00{0, 0, 0, 0, '\0'};
    S r01{0, 1, 2, 1, '\0'};
    S r02{0, 2, 1, 2, '\0'};
    S r10{1, 1, 0, 10, '\0'};
    S r11{1, 1, 1, 11, '\0'};
    S r12{1, 1, 2, 12, '\0'};
    S r20{2, 0, 0, 20, '\0'};
    S r21{2, 1, 1, 21, '\0'};
    S r22{2, 2, 2, 22, '\0'};
    accessor::record_ref ref00{&r00, sizeof(S)};
    accessor::record_ref ref01{&r01, sizeof(S)};
    accessor::record_ref ref02{&r02, sizeof(S)};
    accessor::record_ref ref10{&r10, sizeof(S)};
    accessor::record_ref ref11{&r11, sizeof(S)};
    accessor::record_ref ref12{&r12, sizeof(S)};
    accessor::record_ref ref20{&r20, sizeof(S)};
    accessor::record_ref ref21{&r21, sizeof(S)};
    accessor::record_ref ref22{&r22, sizeof(S)};

    partition.write(ref11);
    partition.write(ref10);
    partition.write(ref12);
    partition.write(ref01);
    partition.write(ref00);
    partition.write(ref02);
    partition.write(ref21);
    partition.write(ref20);
    partition.write(ref22);
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    ASSERT_EQ(9, std::distance(t.begin(), t.end()));
    auto it = t.begin();
    mock::basic_record res00{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res01{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res02{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res10{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res11{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res12{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res20{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res21{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res22{accessor::record_ref{*it++, sizeof(S)}, meta};

    EXPECT_EQ(mock::basic_record(ref00, meta), res00);
    EXPECT_EQ(mock::basic_record(ref01, meta), res01);
    EXPECT_EQ(mock::basic_record(ref02, meta), res02);
    EXPECT_EQ(mock::basic_record(ref10, meta), res10);
    EXPECT_EQ(mock::basic_record(ref11, meta), res11);
    EXPECT_EQ(mock::basic_record(ref12, meta), res12);
    EXPECT_EQ(mock::basic_record(ref20, meta), res20);
    EXPECT_EQ(mock::basic_record(ref21, meta), res21);
    EXPECT_EQ(mock::basic_record(ref22, meta), res22);
}

TEST_F(input_partition_test, sort_desc) {
    auto context = std::make_shared<request_context>();
    struct S {
        std::int64_t i1_{};
        std::int64_t i2_{};
        std::int64_t i3_{};
        std::int64_t i4_{};
        char n_[1];
    };
    std::size_t nullity_base = offsetof(S, n_) * bits_per_byte;
    auto meta = std::make_shared<meta::record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{4}.flip(),
        std::vector<std::size_t>{
            offsetof(S, i1_),
            offsetof(S, i2_),
            offsetof(S, i3_),
            offsetof(S, i4_),
        },
        std::vector<std::size_t>{
            nullity_base + 0,
            nullity_base + 1,
            nullity_base + 2,
            nullity_base + 3,
        },
        alignof(S),
        sizeof(S)
    );
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_shared<group_info>(
            meta,
            std::vector<std::size_t>{0},
            std::vector<std::size_t>{1, 2},
            std::vector<ordering>{ordering::descending, ordering::descending}
        ),
        context.get(),
    };

    mock_memory_resource res{};
    S r00{0, 0, 0, 0, '\0'};
    S r01{0, 1, 2, 1, '\0'};
    S r02{0, 2, 1, 2, '\0'};
    S r10{1, 1, 0, 10, '\0'};
    S r11{1, 1, 1, 11, '\0'};
    S r12{1, 1, 2, 12, '\0'};
    S r20{2, 0, 0, 20, '\0'};
    S r21{2, 1, 1, 21, '\0'};
    S r22{2, 2, 2, 22, '\0'};
    accessor::record_ref ref00{&r00, sizeof(S)};
    accessor::record_ref ref01{&r01, sizeof(S)};
    accessor::record_ref ref02{&r02, sizeof(S)};
    accessor::record_ref ref10{&r10, sizeof(S)};
    accessor::record_ref ref11{&r11, sizeof(S)};
    accessor::record_ref ref12{&r12, sizeof(S)};
    accessor::record_ref ref20{&r20, sizeof(S)};
    accessor::record_ref ref21{&r21, sizeof(S)};
    accessor::record_ref ref22{&r22, sizeof(S)};

    partition.write(ref11);
    partition.write(ref10);
    partition.write(ref12);
    partition.write(ref01);
    partition.write(ref00);
    partition.write(ref02);
    partition.write(ref21);
    partition.write(ref20);
    partition.write(ref22);
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    ASSERT_EQ(9, std::distance(t.begin(), t.end()));
    auto it = t.begin();
    mock::basic_record res00{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res01{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res02{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res10{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res11{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res12{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res20{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res21{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res22{accessor::record_ref{*it++, sizeof(S)}, meta};

    EXPECT_EQ(mock::basic_record(ref02, meta), res00);
    EXPECT_EQ(mock::basic_record(ref01, meta), res01);
    EXPECT_EQ(mock::basic_record(ref00, meta), res02);
    EXPECT_EQ(mock::basic_record(ref12, meta), res10);
    EXPECT_EQ(mock::basic_record(ref11, meta), res11);
    EXPECT_EQ(mock::basic_record(ref10, meta), res12);
    EXPECT_EQ(mock::basic_record(ref22, meta), res20);
    EXPECT_EQ(mock::basic_record(ref21, meta), res21);
    EXPECT_EQ(mock::basic_record(ref20, meta), res22);
}

TEST_F(input_partition_test, sort_desc_asc) {
    auto context = std::make_shared<request_context>();
    struct S {
        std::int64_t i1_{};
        std::int64_t i2_{};
        std::int64_t i3_{};
        std::int64_t i4_{};
        char n_[1];
    };
    std::size_t nullity_base = offsetof(S, n_) * bits_per_byte;
    auto meta = std::make_shared<meta::record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{4}.flip(),
        std::vector<std::size_t>{
            offsetof(S, i1_),
            offsetof(S, i2_),
            offsetof(S, i3_),
            offsetof(S, i4_),
        },
        std::vector<std::size_t>{
            nullity_base + 0,
            nullity_base + 1,
            nullity_base + 2,
            nullity_base + 3,
        },
        alignof(S),
        sizeof(S)
    );
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_shared<group_info>(
            meta,
            std::vector<std::size_t>{0},
            std::vector<std::size_t>{1, 2},
            std::vector<ordering>{ordering::descending, ordering::ascending}
        ),
        context.get(),
    };

    mock_memory_resource res{};
    S r00{0, 2, 0, 0, '\0'};
    S r01{0, 2, 1, 1, '\0'};
    S r02{0, 2, 2, 2, '\0'};
    S r10{0, 1, 0, 10, '\0'};
    S r11{0, 1, 1, 11, '\0'};
    S r12{0, 1, 2, 12, '\0'};
    S r20{0, 0, 0, 20, '\0'};
    S r21{0, 0, 1, 21, '\0'};
    S r22{0, 0, 2, 22, '\0'};
    accessor::record_ref ref00{&r00, sizeof(S)};
    accessor::record_ref ref01{&r01, sizeof(S)};
    accessor::record_ref ref02{&r02, sizeof(S)};
    accessor::record_ref ref10{&r10, sizeof(S)};
    accessor::record_ref ref11{&r11, sizeof(S)};
    accessor::record_ref ref12{&r12, sizeof(S)};
    accessor::record_ref ref20{&r20, sizeof(S)};
    accessor::record_ref ref21{&r21, sizeof(S)};
    accessor::record_ref ref22{&r22, sizeof(S)};

    partition.write(ref11);
    partition.write(ref10);
    partition.write(ref12);
    partition.write(ref01);
    partition.write(ref00);
    partition.write(ref02);
    partition.write(ref21);
    partition.write(ref20);
    partition.write(ref22);
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    ASSERT_EQ(9, std::distance(t.begin(), t.end()));
    auto it = t.begin();
    mock::basic_record res00{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res01{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res02{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res10{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res11{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res12{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res20{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res21{accessor::record_ref{*it++, sizeof(S)}, meta};
    mock::basic_record res22{accessor::record_ref{*it++, sizeof(S)}, meta};

    EXPECT_EQ(mock::basic_record(ref00, meta), res00);
    EXPECT_EQ(mock::basic_record(ref01, meta), res01);
    EXPECT_EQ(mock::basic_record(ref02, meta), res02);
    EXPECT_EQ(mock::basic_record(ref10, meta), res10);
    EXPECT_EQ(mock::basic_record(ref11, meta), res11);
    EXPECT_EQ(mock::basic_record(ref12, meta), res12);
    EXPECT_EQ(mock::basic_record(ref20, meta), res20);
    EXPECT_EQ(mock::basic_record(ref21, meta), res21);
    EXPECT_EQ(mock::basic_record(ref22, meta), res22);
}

}

