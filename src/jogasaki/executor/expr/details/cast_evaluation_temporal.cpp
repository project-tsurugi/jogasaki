/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "cast_evaluation_temporal.h"

#include <takatori/datetime/conversion.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>

#include "common.h"

namespace jogasaki::executor::expr::details::from_character {

any to_date(std::string_view s, evaluator_context& ctx) {
    auto trimmed = trim_spaces(s);
    auto res = takatori::datetime::parse_date(trimmed);
    if(res.is_error()) {
        auto& e = ctx.add_error({error_kind::format_error, res.error()});
        e.new_argument() << s;
        return {std::in_place_type<error>, error(error_kind::format_error)};
    }
    return {std::in_place_type<takatori::datetime::date>,
        takatori::datetime::date{
            static_cast<std::int32_t>(res.value().year),
            res.value().month,
            res.value().day,
        }
    };
}

any to_time_of_day(std::string_view s, evaluator_context& ctx) {
    auto trimmed = trim_spaces(s);
    auto res = takatori::datetime::parse_time(trimmed);
    if(res.is_error()) {
        auto& e = ctx.add_error({error_kind::format_error, res.error()});
        e.new_argument() << s;
        return {std::in_place_type<error>, error(error_kind::format_error)};
    }
    return {std::in_place_type<takatori::datetime::time_of_day>,
        takatori::datetime::time_of_day{
            res.value().hour,
            res.value().minute,
            res.value().second,
            res.value().subsecond,
        }
    };
}

any to_time_point(std::string_view s, bool with_time_zone, evaluator_context& ctx) {
    auto trimmed = trim_spaces(s);
    auto res = takatori::datetime::parse_datetime(trimmed);
    if(res.is_error()) {
        auto& e = ctx.add_error({error_kind::format_error, res.error()});
        e.new_argument() << s;
        return {std::in_place_type<error>, error(error_kind::format_error)};
    }
    if (res.value().offset.has_value() && ! with_time_zone) {
        auto& e = ctx.add_error({error_kind::format_error, "time zone specified for time point without time zone"});
        e.new_argument() << s;
        return {std::in_place_type<error>, error(error_kind::format_error)};
    }

    takatori::datetime::time_point tp{
        takatori::datetime::date{
            static_cast<std::int32_t>(res.value().date.year),
            res.value().date.month,
            res.value().date.day,
        },
        takatori::datetime::time_of_day{
            res.value().time.hour,
            res.value().time.minute,
            res.value().time.second,
            res.value().time.subsecond,
        }
    };

    if(res.value().offset.has_value()) {
        // then with_time_zone=true
        auto& offset = *res.value().offset;
        std::chrono::minutes mins{offset.hour * 60 + offset.minute};
        if (offset.plus) {
            tp -= mins;
        } else {
            tp += mins;
        }
    } else if (with_time_zone) {
        // no zone offset specified, but the target type is with time zone. Use the global zone offset.
        std::chrono::minutes mins{global::config_pool()->zone_offset()};
        tp -= mins;
    }
    return {std::in_place_type<takatori::datetime::time_point>, tp};
}

}  // namespace jogasaki::executor::expr::details::from_character
