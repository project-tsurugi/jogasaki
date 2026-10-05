/* Copyright 2026 Project Tsurugi. Licensed under the Apache License, Version 2.0. */
#include <chrono>
#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include <takatori/type/bit.h>
#include <takatori/type/character.h>
#include <takatori/type/datetime_interval.h>
#include <takatori/value/bit.h>
#include <takatori/value/datetime_interval.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/copy_field_data.h>

namespace jogasaki {
namespace t = takatori::type;
using data::any;
using interval = takatori::datetime::datetime_interval;
using date_part = takatori::datetime::date_interval;
using time_part = takatori::datetime::time_interval;
using namespace std::chrono_literals;

class bit_interval_value_test : public ::testing::Test {
protected:
    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};
    any bits(std::string_view value) { return any{std::in_place_type<accessor::bit>, accessor::bit{&resource_, value}}; }
    static any duration(interval value) { return any{std::in_place_type<interval>, value}; }
    static std::string bit_string(any const& value) { auto bit = value.to<accessor::bit>(); return std::string{static_cast<std::string_view>(bit)}; }
};

TEST_F(bit_interval_value_test, literal_values_preserve_bit_length_and_interval_components) {
    for (auto const& text : {std::string{}, std::string{"1010101"}, std::string(137, '1')}) {
        auto value = utils::as_any(takatori::value::bit{text}, t::bit{t::varying, 200}, &resource_);
        ASSERT_TRUE(value);
        EXPECT_EQ(text, bit_string(value));
        EXPECT_EQ(text.size(), value.to<accessor::bit>().size());
        EXPECT_EQ("bit", data::type_name(value));
    }
    interval expected{date_part{1, -2, 3}, time_part{-123456789ns}};
    auto value = utils::as_any(takatori::value::datetime_interval{expected}, t::datetime_interval{}, &resource_);
    EXPECT_EQ(duration(expected), value);
    EXPECT_EQ("datetime_interval", data::type_name(value));
    std::ostringstream out;
    out << value;
    EXPECT_FALSE(out.str().empty());
}

TEST_F(bit_interval_value_test, long_bit_requires_resource) {
    EXPECT_THROW((void)accessor::bit(nullptr, std::string(16, '1')), std::invalid_argument);
    EXPECT_EQ(15, accessor::bit(nullptr, std::string(15, '1')).size());
}

TEST_F(bit_interval_value_test, copied_record_survives_source_resource_release) {
    auto meta = std::make_shared<meta::record_meta>(std::vector<meta::field_type>{
        meta::field_type{meta::field_enum_tag<meta::field_type_kind::bit>},
        meta::field_type{meta::field_enum_tag<meta::field_type_kind::time_interval>}},
        boost::dynamic_bitset<std::uint64_t>{2}.flip());
    data::small_record_store source{meta}, target{meta};
    interval expected{date_part{-1, 2, -3}, time_part{123456789ns}};
    std::string text(137, '1');
    any copied{};
    {
        memory::lifo_paged_memory_resource temporary{&pool_};
        utils::copy_nullable_field(meta->at(0), source.ref(), meta->value_offset(0), meta->nullity_offset(0),
            any{std::in_place_type<accessor::bit>, accessor::bit{&temporary, text}});
        utils::copy_nullable_field(meta->at(1), source.ref(), meta->value_offset(1), meta->nullity_offset(1), duration(expected));
        for (std::size_t i = 0; i < 2; ++i) {
            utils::copy_nullable_field(meta->at(i), target.ref(), meta->value_offset(i), meta->nullity_offset(i),
                source.ref(), meta->value_offset(i), meta->nullity_offset(i), &resource_);
        }
        utils::copy_nullable_field_as_any(meta->at(0), source.ref(), meta->value_offset(0), meta->nullity_offset(0), copied, &resource_);
        auto original = source.ref().get_value<accessor::bit>(meta->value_offset(0));
        auto duplicate = target.ref().get_value<accessor::bit>(meta->value_offset(0));
        EXPECT_NE(static_cast<std::string_view>(original).data(), static_cast<std::string_view>(duplicate).data());
    }
    any from_record{};
    utils::copy_nullable_field_as_any(meta->at(0), target.ref(), meta->value_offset(0), meta->nullity_offset(0), from_record, &resource_);
    EXPECT_EQ(text, bit_string(from_record));
    EXPECT_EQ(text, bit_string(copied));
    utils::copy_nullable_field_as_any(meta->at(1), target.ref(), meta->value_offset(1), meta->nullity_offset(1), from_record);
    EXPECT_EQ(duration(expected), from_record);
    EXPECT_FALSE(target.ref().is_null(meta->nullity_offset(0)));
}

TEST_F(bit_interval_value_test, nullable_copy_preserves_null_for_both_types) {
    for (auto const& field : {meta::field_type{meta::field_enum_tag<meta::field_type_kind::bit>},
                             meta::field_type{meta::field_enum_tag<meta::field_type_kind::time_interval>}}) {
        auto meta = std::make_shared<meta::record_meta>(std::vector<meta::field_type>{field}, boost::dynamic_bitset<std::uint64_t>{1}.flip());
        data::small_record_store source{meta}, target{meta};
        utils::copy_nullable_field(field, source.ref(), meta->value_offset(0), meta->nullity_offset(0), any{}, &resource_);
        utils::copy_nullable_field(field, target.ref(), meta->value_offset(0), meta->nullity_offset(0),
            source.ref(), meta->value_offset(0), meta->nullity_offset(0), &resource_);
        any result = bits("1");
        utils::copy_nullable_field_as_any(field, target.ref(), meta->value_offset(0), meta->nullity_offset(0), result, &resource_);
        EXPECT_TRUE(target.ref().is_null(meta->nullity_offset(0)));
        EXPECT_TRUE(result.empty());
    }
}

}
