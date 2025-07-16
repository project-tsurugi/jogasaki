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
#include "create_default_value.h"

#include <cstdlib>
#include <memory>

#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/varying.h>
#include <takatori/value/value_kind.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::conv {

using namespace jogasaki::executor::process::impl;

std::shared_ptr<::takatori::type::data const> literal_type(::takatori::value::data const& value) {
    using k = ::takatori::value::value_kind;
    switch (value.kind()) {
        case k::unknown:
            return std::make_shared<::takatori::type::unknown>();
        case k::boolean:
            return std::make_shared<::takatori::type::boolean>();
        case k::int4:
            return std::make_shared<::takatori::type::int4>();
        case k::int8:
            return std::make_shared<::takatori::type::int8>();
        case k::float4:
            return std::make_shared<::takatori::type::float4>();
        case k::float8:
            return std::make_shared<::takatori::type::float8>();
        case k::decimal:
            return std::make_shared<::takatori::type::decimal>();
        case k::character:
            return std::make_shared<::takatori::type::character>(::takatori::type::varying);
        case k::octet:
            return std::make_shared<::takatori::type::octet>(::takatori::type::varying);
        case k::date:
            return std::make_shared<::takatori::type::date>();
        case k::time_of_day:
            return std::make_shared<::takatori::type::time_of_day>();
        case k::time_point:
            return std::make_shared<::takatori::type::time_point>();
        case k::bit:
        case k::datetime_interval:
        case k::array:
        case k::record:
        case k::extension:
            // TODO: support
            return {};
    }
    fail_with_exception();
}

data::any create_immediate_default_value(
    takatori::value::data const& value,
    takatori::type::data const& type,
    memory::lifo_paged_memory_resource* resource
) {
    auto lt = literal_type(value);
    auto a = utils::as_any(
        value,
        *lt,
        nullptr // varlen data is owned by takatori so resource is not required
    );
    if(conv::to_require_conversion(*lt, type)) {
        // constant assignment conversion is required
        expr::evaluator_context ectx{resource, nullptr};  // immediate value won't evaluate any function
        ectx.set_loss_precision_policy(expr::loss_precision_policy::ignore);
        return expr::details::conduct_cast(ectx, *lt, type, a);
    }
    return a;
}

}  // namespace jogasaki::executor::conv
