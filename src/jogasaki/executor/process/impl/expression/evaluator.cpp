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
#include "evaluator.h"

#include <jogasaki/accessor/text.h>

namespace jogasaki::executor::process::impl::expression {

using takatori::util::fail;

namespace details {

template <>
void callback::binary<accessor::text>(takatori::scalar::binary_operator op, stack_type& stack, memory_resource* resource) {
    using kind = takatori::scalar::binary_operator;
    auto right = pop<accessor::text>(stack);
    auto left = pop<accessor::text>(stack);
    if (op == kind::concat) {
        push<accessor::text>(stack, accessor::text{resource, left, right});
        return;
    }
    fail();
}

void callback::operator()(takatori::util::post_visit, const takatori::scalar::binary &arg,
    callback::stack_type &stack, callback::memory_resource *resource) {
    auto& type = info_.type_of(arg.left()); //TODO support cases where left/right types differ
    using t = takatori::type::type_kind;
    switch(type.kind()) {
        case t::int4: binary<std::int32_t>(arg.operator_kind(), stack, resource); break;
        case t::int8: binary<std::int64_t>(arg.operator_kind(), stack, resource); break;
        case t::float4: binary<float>(arg.operator_kind(), stack, resource); break;
        case t::float8: binary<double>(arg.operator_kind(), stack, resource); break;
        case t::boolean: binary<bool>(arg.operator_kind(), stack, resource); break;
        case t::character : binary<accessor::text>(arg.operator_kind(), stack, resource); break;
        default: fail();
    }
}
}

} // namespace
