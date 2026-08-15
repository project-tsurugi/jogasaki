/*
 * Copyright 2018-2025 Project Tsurugi.
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
#pragma once

#include <functional>

#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/value_store.h>
#include <jogasaki/executor/function/scalar_function_repository.h>

namespace jogasaki::executor::function {

/**
 * @brief register built-in scalar functions to the given provider and function repository
 * @param functions the provider where the built-in functions are registered
 * @param repo the function repository where the built-in functions are registered
 */
void add_builtin_scalar_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo
);

namespace builtin {

using takatori::util::sequence_view;

data::any octet_length(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any current_date(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any localtime(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any current_timestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any localtimestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any substring(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any upper(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any lower(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any character_length(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any abs(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any position(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any mod(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any ceil(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any floor(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any round(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any encode(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any decode(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any rtrim(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

data::any ltrim(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the year field from date or timestamp (without time zone)
 */
data::any extract_year(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the year field from timestamp with time zone (in the system time zone)
 */
data::any extract_year_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the month field from date or timestamp (without time zone)
 */
data::any extract_month(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the month field from timestamp with time zone (in the system time zone)
 */
data::any extract_month_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the day field from date or timestamp (without time zone)
 */
data::any extract_day(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the day field from timestamp with time zone (in the system time zone)
 */
data::any extract_day_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the hour field from time or timestamp (without time zone)
 */
data::any extract_hour(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the hour field from timestamp with time zone (in the system time zone)
 */
data::any extract_hour_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the minute field from time or timestamp (without time zone)
 */
data::any extract_minute(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the minute field from timestamp with time zone (in the system time zone)
 */
data::any extract_minute_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the second field (with the precision given as 2nd arg) from time or timestamp (without time zone)
 */
data::any extract_second(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the second field (with the precision given as 2nd arg) from timestamp with time zone
 */
data::any extract_second_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the hour part of the system time zone offset
 */
data::any extract_timezone_hour(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the minute part of the system time zone offset
 */
data::any extract_timezone_minute(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the date part from date or timestamp (without time zone)
 */
data::any extract_date(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the date part from timestamp with time zone (in the system time zone)
 */
data::any extract_date_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the year/month prefix (with day set to 1) from date or timestamp (without time zone)
 */
data::any extract_year_to_month(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief extract the year/month prefix (with day set to 1) from timestamp with time zone
 */
data::any extract_year_to_month_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief truncate the fields less than hour from timestamp (without time zone)
 */
data::any extract_year_to_hour(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief truncate the fields less than hour (in the system time zone) from timestamp with time zone
 */
data::any extract_year_to_hour_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief truncate the fields less than minute from timestamp (without time zone)
 */
data::any extract_year_to_minute(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief truncate the fields less than minute (in the system time zone) from timestamp with time zone
 */
data::any extract_year_to_minute_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief truncate the subsecond digits below the precision (given as 2nd arg) from timestamp (without time zone)
 */
data::any extract_year_to_second(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

/**
 * @brief truncate the subsecond digits below the precision (given as 2nd arg) from timestamp with time zone
 */
data::any extract_year_to_second_with_time_zone(
    evaluator_context& ctx,
    sequence_view<data::any> args
);

}  // namespace builtin

}  // namespace jogasaki::executor::function
