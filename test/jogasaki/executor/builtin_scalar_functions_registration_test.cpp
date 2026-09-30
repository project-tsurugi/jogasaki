/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/varying.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/builtin_scalar_functions.h>
#include <jogasaki/executor/function/builtin_scalar_functions_id.h>
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/scalar_function_repository.h>

namespace jogasaki::executor::function {

namespace t = takatori::type;

using kind = scalar_function_kind;
using takatori::util::sequence_view;
using executor::expr::evaluator_context;

/**
 * @brief golden test to verify the built-in scalar function registration
 * @details the function ids are possibly made durable (e.g. scalar function in the DEFAULT clause), so the mapping
 * from id to the function signature and implementation must not be changed accidentally.
 * When you add a new built-in scalar function, append the new entries at the end of the expected list.
 */
class builtin_scalar_functions_registration_test : public ::testing::Test {};

using body_ptr = data::any (*)(evaluator_context&, sequence_view<data::any>);
using type_ptr = std::shared_ptr<t::data const>;

struct expected_entry {
    std::size_t id_;
    std::string_view name_;
    scalar_function_kind kind_;
    body_ptr body_;
    std::size_t arg_count_;
    type_ptr return_type_;
    std::vector<type_ptr> parameter_types_;
};

template <class T, class... Args>
type_ptr make(Args&&... args) {
    return std::make_shared<T>(std::forward<Args>(args)...);
}

std::vector<expected_entry> expected_entries() {
    return {
        {scalar_function_id::id_11000, "octet_length", kind::octet_length, builtin::octet_length, 1, make<t::int8>(), {make<t::character>(t::varying)}},
        {scalar_function_id::id_11001, "octet_length", kind::octet_length, builtin::octet_length, 1, make<t::int8>(), {make<t::octet>(t::varying)}},
        {scalar_function_id::id_11002, "current_date", kind::current_date, builtin::current_date, 0, make<t::date>(), {}},
        {scalar_function_id::id_11003, "localtime", kind::localtime, builtin::localtime, 0, make<t::time_of_day>(), {}},
        {scalar_function_id::id_11004, "current_timestamp", kind::current_timestamp, builtin::current_timestamp, 0, make<t::time_point>(t::with_time_zone), {}},
        {scalar_function_id::id_11005, "localtimestamp", kind::localtimestamp, builtin::localtimestamp, 0, make<t::time_point>(), {}},
        {scalar_function_id::id_11006, "substring", kind::substring, builtin::substring, 3, make<t::character>(t::varying), {make<t::character>(t::varying), make<t::int8>(), make<t::int8>()}},
        {scalar_function_id::id_11007, "substring", kind::substring, builtin::substring, 2, make<t::character>(t::varying), {make<t::character>(t::varying), make<t::int8>()}},
        {scalar_function_id::id_11008, "substring", kind::substring, builtin::substring, 3, make<t::octet>(t::varying), {make<t::octet>(t::varying), make<t::int8>(), make<t::int8>()}},
        {scalar_function_id::id_11009, "substring", kind::substring, builtin::substring, 2, make<t::octet>(t::varying), {make<t::octet>(t::varying), make<t::int8>()}},
        {scalar_function_id::id_11010, "upper", kind::upper, builtin::upper, 1, make<t::character>(t::varying), {make<t::character>(t::varying)}},
        {scalar_function_id::id_11011, "lower", kind::lower, builtin::lower, 1, make<t::character>(t::varying), {make<t::character>(t::varying)}},
        {scalar_function_id::id_11012, "character_length", kind::character_length, builtin::character_length, 1, make<t::int8>(), {make<t::character>(t::varying)}},
        {scalar_function_id::id_11013, "char_length", kind::char_length, builtin::character_length, 1, make<t::int8>(), {make<t::character>(t::varying)}},
        {scalar_function_id::id_11014, "abs", kind::abs, builtin::abs, 1, make<t::int4>(), {make<t::int4>()}},
        {scalar_function_id::id_11015, "abs", kind::abs, builtin::abs, 1, make<t::int8>(), {make<t::int8>()}},
        {scalar_function_id::id_11016, "abs", kind::abs, builtin::abs, 1, make<t::float4>(), {make<t::float4>()}},
        {scalar_function_id::id_11017, "abs", kind::abs, builtin::abs, 1, make<t::float8>(), {make<t::float8>()}},
        {scalar_function_id::id_11018, "abs", kind::abs, builtin::abs, 1, make<t::decimal>(), {make<t::decimal>()}},
        {scalar_function_id::id_11019, "position", kind::position, builtin::position, 2, make<t::int8>(), {make<t::character>(t::varying), make<t::character>(t::varying)}},
        {scalar_function_id::id_11020, "mod", kind::mod, builtin::mod, 2, make<t::int4>(), {make<t::int4>(), make<t::int4>()}},
        {scalar_function_id::id_11021, "mod", kind::mod, builtin::mod, 2, make<t::int8>(), {make<t::int4>(), make<t::int8>()}},
        {scalar_function_id::id_11022, "mod", kind::mod, builtin::mod, 2, make<t::int8>(), {make<t::int8>(), make<t::int4>()}},
        {scalar_function_id::id_11023, "mod", kind::mod, builtin::mod, 2, make<t::int8>(), {make<t::int8>(), make<t::int8>()}},
        {scalar_function_id::id_11024, "mod", kind::mod, builtin::mod, 2, make<t::decimal>(), {make<t::int4>(), make<t::decimal>()}},
        {scalar_function_id::id_11025, "mod", kind::mod, builtin::mod, 2, make<t::decimal>(), {make<t::decimal>(), make<t::int4>()}},
        {scalar_function_id::id_11026, "mod", kind::mod, builtin::mod, 2, make<t::decimal>(), {make<t::decimal>(), make<t::int8>()}},
        {scalar_function_id::id_11027, "mod", kind::mod, builtin::mod, 2, make<t::decimal>(), {make<t::int8>(), make<t::decimal>()}},
        {scalar_function_id::id_11028, "mod", kind::mod, builtin::mod, 2, make<t::decimal>(), {make<t::decimal>(), make<t::decimal>()}},
        {scalar_function_id::id_11029, "substr", kind::substr, builtin::substring, 3, make<t::character>(t::varying), {make<t::character>(t::varying), make<t::int8>(), make<t::int8>()}},
        {scalar_function_id::id_11030, "substr", kind::substr, builtin::substring, 2, make<t::character>(t::varying), {make<t::character>(t::varying), make<t::int8>()}},
        {scalar_function_id::id_11031, "substr", kind::substr, builtin::substring, 3, make<t::octet>(t::varying), {make<t::octet>(t::varying), make<t::int8>(), make<t::int8>()}},
        {scalar_function_id::id_11032, "substr", kind::substr, builtin::substring, 2, make<t::octet>(t::varying), {make<t::octet>(t::varying), make<t::int8>()}},
        {scalar_function_id::id_11033, "ceil", kind::ceil, builtin::ceil, 1, make<t::int4>(), {make<t::int4>()}},
        {scalar_function_id::id_11034, "ceil", kind::ceil, builtin::ceil, 1, make<t::int8>(), {make<t::int8>()}},
        {scalar_function_id::id_11035, "ceil", kind::ceil, builtin::ceil, 1, make<t::float4>(), {make<t::float4>()}},
        {scalar_function_id::id_11036, "ceil", kind::ceil, builtin::ceil, 1, make<t::float8>(), {make<t::float8>()}},
        {scalar_function_id::id_11037, "ceil", kind::ceil, builtin::ceil, 1, make<t::decimal>(), {make<t::decimal>()}},
        {scalar_function_id::id_11038, "floor", kind::floor, builtin::floor, 1, make<t::int4>(), {make<t::int4>()}},
        {scalar_function_id::id_11039, "floor", kind::floor, builtin::floor, 1, make<t::int8>(), {make<t::int8>()}},
        {scalar_function_id::id_11040, "floor", kind::floor, builtin::floor, 1, make<t::float4>(), {make<t::float4>()}},
        {scalar_function_id::id_11041, "floor", kind::floor, builtin::floor, 1, make<t::float8>(), {make<t::float8>()}},
        {scalar_function_id::id_11042, "floor", kind::floor, builtin::floor, 1, make<t::decimal>(), {make<t::decimal>()}},
        {scalar_function_id::id_11043, "round", kind::round, builtin::round, 1, make<t::int4>(), {make<t::int4>()}},
        {scalar_function_id::id_11044, "round", kind::round, builtin::round, 1, make<t::int8>(), {make<t::int8>()}},
        {scalar_function_id::id_11045, "round", kind::round, builtin::round, 1, make<t::float4>(), {make<t::float4>()}},
        {scalar_function_id::id_11046, "round", kind::round, builtin::round, 1, make<t::float8>(), {make<t::float8>()}},
        {scalar_function_id::id_11047, "round", kind::round, builtin::round, 1, make<t::decimal>(), {make<t::decimal>()}},
        {scalar_function_id::id_11048, "round", kind::round, builtin::round, 2, make<t::int4>(), {make<t::int4>(), make<t::int4>()}},
        {scalar_function_id::id_11049, "round", kind::round, builtin::round, 2, make<t::int8>(), {make<t::int8>(), make<t::int4>()}},
        {scalar_function_id::id_11050, "round", kind::round, builtin::round, 2, make<t::float4>(), {make<t::float4>(), make<t::int4>()}},
        {scalar_function_id::id_11051, "round", kind::round, builtin::round, 2, make<t::float8>(), {make<t::float8>(), make<t::int4>()}},
        {scalar_function_id::id_11052, "round", kind::round, builtin::round, 2, make<t::decimal>(), {make<t::decimal>(), make<t::int4>()}},
        {scalar_function_id::id_11053, "round", kind::round, builtin::round, 2, make<t::int4>(), {make<t::int4>(), make<t::int8>()}},
        {scalar_function_id::id_11054, "round", kind::round, builtin::round, 2, make<t::int8>(), {make<t::int8>(), make<t::int8>()}},
        {scalar_function_id::id_11055, "round", kind::round, builtin::round, 2, make<t::float4>(), {make<t::float4>(), make<t::int8>()}},
        {scalar_function_id::id_11056, "round", kind::round, builtin::round, 2, make<t::float8>(), {make<t::float8>(), make<t::int8>()}},
        {scalar_function_id::id_11057, "round", kind::round, builtin::round, 2, make<t::decimal>(), {make<t::decimal>(), make<t::int8>()}},
        {scalar_function_id::id_11058, "encode", kind::encode, builtin::encode, 2, make<t::character>(t::varying), {make<t::octet>(t::varying), make<t::character>(t::varying)}},
        {scalar_function_id::id_11059, "decode", kind::decode, builtin::decode, 2, make<t::octet>(t::varying), {make<t::character>(t::varying), make<t::character>(t::varying)}},
        {scalar_function_id::id_11060, "rtrim", kind::rtrim, builtin::rtrim, 1, make<t::character>(t::varying), {make<t::character>(t::varying)}},
        {scalar_function_id::id_11061, "ltrim", kind::ltrim, builtin::ltrim, 1, make<t::character>(t::varying), {make<t::character>(t::varying)}},
        {scalar_function_id::id_11062, "extract_year", kind::extract_year, builtin::extract_year, 1, make<t::int4>(), {make<t::date>()}},
        {scalar_function_id::id_11063, "extract_year", kind::extract_year, builtin::extract_year, 1, make<t::int4>(), {make<t::time_point>()}},
        {scalar_function_id::id_11064, "extract_year", kind::extract_year, builtin::extract_year_with_time_zone, 1, make<t::int4>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11065, "extract_month", kind::extract_month, builtin::extract_month, 1, make<t::int4>(), {make<t::date>()}},
        {scalar_function_id::id_11066, "extract_month", kind::extract_month, builtin::extract_month, 1, make<t::int4>(), {make<t::time_point>()}},
        {scalar_function_id::id_11067, "extract_month", kind::extract_month, builtin::extract_month_with_time_zone, 1, make<t::int4>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11068, "extract_day", kind::extract_day, builtin::extract_day, 1, make<t::int4>(), {make<t::date>()}},
        {scalar_function_id::id_11069, "extract_day", kind::extract_day, builtin::extract_day, 1, make<t::int4>(), {make<t::time_point>()}},
        {scalar_function_id::id_11070, "extract_day", kind::extract_day, builtin::extract_day_with_time_zone, 1, make<t::int4>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11071, "extract_hour", kind::extract_hour, builtin::extract_hour, 1, make<t::int4>(), {make<t::time_of_day>()}},
        {scalar_function_id::id_11072, "extract_hour", kind::extract_hour, builtin::extract_hour, 1, make<t::int4>(), {make<t::time_point>()}},
        {scalar_function_id::id_11073, "extract_hour", kind::extract_hour, builtin::extract_hour_with_time_zone, 1, make<t::int4>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11074, "extract_minute", kind::extract_minute, builtin::extract_minute, 1, make<t::int4>(), {make<t::time_of_day>()}},
        {scalar_function_id::id_11075, "extract_minute", kind::extract_minute, builtin::extract_minute, 1, make<t::int4>(), {make<t::time_point>()}},
        {scalar_function_id::id_11076, "extract_minute", kind::extract_minute, builtin::extract_minute_with_time_zone, 1, make<t::int4>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11077, "extract_second", kind::extract_second, builtin::extract_second, 2, make<t::decimal>(), {make<t::time_of_day>(), make<t::int4>()}},
        {scalar_function_id::id_11078, "extract_second", kind::extract_second, builtin::extract_second, 2, make<t::decimal>(), {make<t::time_point>(), make<t::int4>()}},
        {scalar_function_id::id_11079, "extract_second", kind::extract_second, builtin::extract_second_with_time_zone, 2, make<t::decimal>(), {make<t::time_point>(t::with_time_zone), make<t::int4>()}},
        {scalar_function_id::id_11080, "extract_timezone_hour", kind::extract_timezone_hour, builtin::extract_timezone_hour, 1, make<t::int4>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11081, "extract_timezone_minute", kind::extract_timezone_minute, builtin::extract_timezone_minute, 1, make<t::int4>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11082, "date", kind::date, builtin::extract_date, 1, make<t::date>(), {make<t::date>()}},
        {scalar_function_id::id_11083, "date", kind::date, builtin::extract_date, 1, make<t::date>(), {make<t::time_point>()}},
        {scalar_function_id::id_11084, "date", kind::date, builtin::extract_date_with_time_zone, 1, make<t::date>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11085, "extract_year_to_month", kind::extract_year_to_month, builtin::extract_year_to_month, 1, make<t::date>(), {make<t::date>()}},
        {scalar_function_id::id_11086, "extract_year_to_month", kind::extract_year_to_month, builtin::extract_year_to_month, 1, make<t::date>(), {make<t::time_point>()}},
        {scalar_function_id::id_11087, "extract_year_to_month", kind::extract_year_to_month, builtin::extract_year_to_month_with_time_zone, 1, make<t::date>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11088, "extract_year_to_day", kind::extract_year_to_day, builtin::extract_date, 1, make<t::date>(), {make<t::date>()}},
        {scalar_function_id::id_11089, "extract_year_to_day", kind::extract_year_to_day, builtin::extract_date, 1, make<t::date>(), {make<t::time_point>()}},
        {scalar_function_id::id_11090, "extract_year_to_day", kind::extract_year_to_day, builtin::extract_date_with_time_zone, 1, make<t::date>(), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11091, "extract_year_to_hour", kind::extract_year_to_hour, builtin::extract_year_to_hour, 1, make<t::time_point>(), {make<t::time_point>()}},
        {scalar_function_id::id_11092, "extract_year_to_hour", kind::extract_year_to_hour, builtin::extract_year_to_hour_with_time_zone, 1, make<t::time_point>(t::with_time_zone), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11093, "extract_year_to_minute", kind::extract_year_to_minute, builtin::extract_year_to_minute, 1, make<t::time_point>(), {make<t::time_point>()}},
        {scalar_function_id::id_11094, "extract_year_to_minute", kind::extract_year_to_minute, builtin::extract_year_to_minute_with_time_zone, 1, make<t::time_point>(t::with_time_zone), {make<t::time_point>(t::with_time_zone)}},
        {scalar_function_id::id_11095, "extract_year_to_second", kind::extract_year_to_second, builtin::extract_year_to_second, 2, make<t::time_point>(), {make<t::time_point>(), make<t::int4>()}},
        {scalar_function_id::id_11096, "extract_year_to_second", kind::extract_year_to_second, builtin::extract_year_to_second_with_time_zone, 2, make<t::time_point>(t::with_time_zone), {make<t::time_point>(t::with_time_zone), make<t::int4>()}},
    };
}

TEST_F(builtin_scalar_functions_registration_test, golden) {
    ::yugawara::function::configurable_provider functions{};
    scalar_function_repository repo{};
    add_builtin_scalar_functions(functions, repo);

    auto expected = expected_entries();
    ASSERT_EQ(expected.size(), repo.size());

    // provider iterates declarations ordered by name, keeping the registration order among the overloads
    std::vector<std::shared_ptr<::yugawara::function::declaration const>> decls{};
    functions.each([&](std::shared_ptr<::yugawara::function::declaration const> const& decl) {
        decls.emplace_back(decl);
    });
    ASSERT_EQ(expected.size(), decls.size());

    auto ordered = expected;
    std::stable_sort(ordered.begin(), ordered.end(), [](auto const& a, auto const& b) {
        return a.name_ < b.name_;
    });
    ::yugawara::function::declaration const default_decl{0, "dummy", t::int4(), {}};
    for(std::size_t i = 0; i < ordered.size(); ++i) {
        auto const& e = ordered[i];
        auto const& decl = *decls[i];
        SCOPED_TRACE(::testing::Message() << "id:" << e.id_ << " name:" << e.name_);
        EXPECT_EQ(e.id_, decl.definition_id());
        EXPECT_EQ(e.name_, decl.name());
        EXPECT_EQ(*e.return_type_, decl.return_type());
        auto params = decl.parameter_types();
        ASSERT_EQ(e.parameter_types_.size(), params.size());
        for(std::size_t j = 0; j < params.size(); ++j) {
            EXPECT_EQ(*e.parameter_types_[j], params[j]) << "parameter index:" << j;
        }
        EXPECT_EQ(default_decl.features(), decl.features());
        EXPECT_EQ(default_decl.description(), decl.description());

        auto const* info = repo.find(e.id_);
        ASSERT_TRUE(info);
        EXPECT_EQ(e.kind_, info->kind());
        EXPECT_EQ(e.arg_count_, info->arg_count());
        auto const* body = info->function_body().target<body_ptr>();
        ASSERT_TRUE(body);
        EXPECT_EQ(e.body_, *body);
    }
}

}  // namespace jogasaki::executor::function
