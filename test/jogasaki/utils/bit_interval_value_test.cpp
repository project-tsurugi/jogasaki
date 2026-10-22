/* Copyright 2026 Project Tsurugi. Licensed under the Apache License, Version 2.0. */
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/type/bit.h>
#include <takatori/type/character.h>
#include <takatori/type/datetime_interval.h>
#include <takatori/value/bit.h>
#include <takatori/value/datetime_interval.h>
#include <yugawara/function/configurable_provider.h>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/wrt/fill_record_fields.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/validate_any_type.h>

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

TEST_F(bit_interval_value_test, copied_bit_checks_resource_at_inline_boundary) {
    for (std::size_t length : {0, 7, 15, 16, 137}) {
        std::string text(length, '1');
        accessor::bit source{&resource_, text};
        if (length <= 15) {
            accessor::bit copied{nullptr, source};
            EXPECT_EQ(text, static_cast<std::string_view>(copied));
        } else {
            EXPECT_THROW((void)accessor::bit(nullptr, source), std::invalid_argument);
        }
        accessor::bit copied{&resource_, source};
        EXPECT_EQ(text, static_cast<std::string_view>(copied));
        if (length > 15) {
            EXPECT_NE(static_cast<std::string_view>(source).data(), static_cast<std::string_view>(copied).data());
        }
    }
}

TEST_F(bit_interval_value_test, bit_construction_rejects_non_binary_characters) {
    for (auto const& text : {std::string{"abc"}, std::string{"102"}, std::string{" 01"},
                             std::string{"01\n"}, std::string{"01\0", 3}, std::string(16, 'x')}) {
        EXPECT_THROW((void)accessor::bit(&resource_, text), std::invalid_argument);
        EXPECT_THROW((void)accessor::bit(nullptr, text), std::invalid_argument);
    }
    for (auto const& text : {std::string{}, std::string{"0"}, std::string{"1"}, std::string{"1010101"}, std::string(137, '0')}) {
        accessor::bit value{&resource_, text};
        EXPECT_EQ(text, static_cast<std::string_view>(value));
    }
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

TEST_F(bit_interval_value_test, validate_matching_and_mismatching_types) {
    auto bit_type = meta::field_type{meta::field_enum_tag<meta::field_type_kind::bit>};
    auto interval_type = meta::field_type{meta::field_enum_tag<meta::field_type_kind::time_interval>};
    auto bit_value = bits("1010101");
    auto interval_value = duration(interval{date_part{1, -2, 3}, time_part{-123456789ns}});
    EXPECT_TRUE(utils::validate_any_type(bit_value, bit_type));
    EXPECT_TRUE(utils::validate_any_type(interval_value, interval_type));
    EXPECT_FALSE(utils::validate_any_type(bit_value, interval_type));
    EXPECT_FALSE(utils::validate_any_type(interval_value, bit_type));
    EXPECT_TRUE(utils::validate_any_type(any{}, bit_type));
    EXPECT_TRUE(utils::validate_any_type(any{}, interval_type));
}

TEST_F(bit_interval_value_test, default_functions_write_values_and_nulls) {
    constexpr std::size_t function_id = 9000001;
    auto& repository = global::scalar_function_repository();
    ASSERT_EQ(nullptr, repository.find(function_id));
    struct restore_repository {
        executor::function::scalar_function_repository& repository;
        ~restore_repository() { repository = saved; }
        executor::function::scalar_function_repository saved{repository};
    } restore{repository};
    interval expected{date_part{-1, 2, -3}, time_part{123456789ns}};
    std::string text(137, '1');
    for (bool bit_type : {true, false}) {
        std::unique_ptr<t::data> type = bit_type
            ? std::unique_ptr<t::data>{std::make_unique<t::bit>(t::varying, 200)}
            : std::unique_ptr<t::data>{std::make_unique<t::datetime_interval>()};
        auto field_type = utils::type_for(*type);
        auto meta = std::make_shared<meta::record_meta>(std::vector<meta::field_type>{field_type},
            boost::dynamic_bitset<std::uint64_t>{1}.flip());
        yugawara::function::configurable_provider functions{};
        if (bit_type) { functions.add({function_id, "test_runtime_default", t::bit{t::varying, 200}, {}}); }
        else { functions.add({function_id, "test_runtime_default", t::datetime_interval{}, {}}); }
        executor::wrt::write_field field{0, *type, kvs::spec_value, true,
            meta->value_offset(0), meta->nullity_offset(0),
            executor::process::impl::ops::default_value_kind::function, any{}, function_id, &functions};
        request_context context{};
        context.transaction(std::make_shared<transaction_context>(nullptr));
        data::small_record_store record{meta};
        repository = restore.saved;
        bool called = false;
        bool null_value = false;
        bool wrong_type = false;
        repository.add(function_id, std::make_shared<executor::function::scalar_function_info>(
            executor::function::scalar_function_kind::user_defined,
            [&](executor::expr::evaluator_context& ctx, takatori::util::sequence_view<any>) {
                called = true;
                if (wrong_type) { return any{std::in_place_type<std::int64_t>, 1}; }
                if (null_value) { return any{}; }
                return bit_type ? any{std::in_place_type<accessor::bit>, accessor::bit{ctx.resource(), text}}
                                : duration(expected);
            }, 0));
        for (bool produce_null : {false, true}) {
            null_value = produce_null;
            called = false;
            ASSERT_EQ(status::ok, executor::wrt::fill_default_value(field, context, resource_, record));
            ASSERT_TRUE(called);
            EXPECT_EQ(null_value, record.ref().is_null(meta->nullity_offset(0)));
            if (!null_value) {
                any value{};
                utils::copy_nullable_field_as_any(field_type, record.ref(), meta->value_offset(0),
                    meta->nullity_offset(0), value, &resource_);
                if (bit_type) { EXPECT_EQ(text, bit_string(value)); }
                else { EXPECT_EQ(duration(expected), value); }
            }
        }
        wrong_type = true;
        EXPECT_EQ(status::err_unsupported, executor::wrt::fill_default_value(field, context, resource_, record));
    }
}

TEST_F(bit_interval_value_test, record_printing_supports_values_and_nulls) {
    for (auto const& field : {meta::field_type{meta::field_enum_tag<meta::field_type_kind::bit>},
                             meta::field_type{meta::field_enum_tag<meta::field_type_kind::time_interval>}}) {
        auto meta = std::make_shared<meta::record_meta>(std::vector<meta::field_type>{field},
            boost::dynamic_bitset<std::uint64_t>{1}.flip());
        data::small_record_store record{meta};
        auto value = field.kind() == meta::field_type_kind::bit ? bits("1010101")
            : duration(interval{date_part{-1, 2, -3}, time_part{123456789ns}});
        utils::copy_nullable_field(field, record.ref(), meta->value_offset(0), meta->nullity_offset(0), value, &resource_);
        auto expected = field.kind() == meta::field_type_kind::bit
            ? "(0:bit*)[1010101]"
            : "(0:time_interval*)[datetime_interval(-1/+2/-3 +0:+0:+0:+0.123456789)]";
        std::ostringstream actual;
        EXPECT_NO_THROW(actual << record.ref() << *meta);
        EXPECT_EQ(expected, actual.str());
        record.ref().set_null(meta->nullity_offset(0), true);
        std::ostringstream null_output;
        null_output << record.ref() << *meta;
        EXPECT_EQ("(0:" + std::string{to_string_view(field.kind())} + "*)[-]", null_output.str());
    }
}

}
