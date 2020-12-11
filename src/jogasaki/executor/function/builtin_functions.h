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

#include <vector>
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/executor/function/functions.h>

namespace jogasaki::executor::function {

namespace builtin {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

void sum(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
);

void count(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
);

} // namespace builtin

}
