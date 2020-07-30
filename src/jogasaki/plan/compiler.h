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

#include <string_view>

#include <takatori/plan/forward.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>

#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/process/step.h>

#include "compiler_context.h"

namespace jogasaki::plan {

// for testing
namespace impl {
[[nodiscard]] executor::process::step create(takatori::plan::process const& process, compiler_context& ctx);
[[nodiscard]] executor::exchange::forward::step create(takatori::plan::forward const& forward, compiler_context& ctx);
[[nodiscard]] executor::exchange::group::step create(takatori::plan::group const& group, compiler_context& ctx);
}

/**
 * @brief compile sql
 * @param sql the sql statement to compile
 * @param ctx the compiler context filled with storage provider required to compile the sql
 */
[[nodiscard]] bool compile(std::string_view sql, compiler_context& ctx);

}
