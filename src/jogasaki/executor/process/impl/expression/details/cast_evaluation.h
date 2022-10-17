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
#pragma once

#include <cstddef>
#include <functional>

#include <takatori/scalar/expression.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/match.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/coalesce.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/conditional.h>
#include <takatori/scalar/extension.h>
#include <takatori/scalar/function_call.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/let.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/variable_reference.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/data/any.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>

namespace jogasaki::executor::process::impl::expression::details {

using any = jogasaki::data::any;

any conduct_cast(
    evaluator_context& ctx,
    ::takatori::type::data const& src,
    ::takatori::type::data const& tgt,
    any const& a
);

// conversion from string
any to_int1(std::string_view s, evaluator_context& ctx);
any to_int2(std::string_view s, evaluator_context& ctx);
any to_int4(std::string_view s, evaluator_context& ctx);
any to_int8(std::string_view s, evaluator_context& ctx);
any to_float4(std::string_view s, evaluator_context& ctx);
any to_float8(std::string_view s, evaluator_context& ctx);
any to_decimal(std::string_view s, evaluator_context& ctx);

} // namespace
