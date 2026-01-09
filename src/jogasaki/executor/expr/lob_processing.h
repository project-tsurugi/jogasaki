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
#pragma once

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/evaluator_context.h>

namespace jogasaki::executor::expr {

/**
 * @brief Pre-process LOB reference before passing to functions.
 * @details Assigns reference tag to LOB references. If the input is not a LOB type,
 * it is returned as-is.
 * @param in the input value (may be LOB or any other type)
 * @param ctx the evaluator context
 * @return the processed value (with reference tag assigned if LOB), or error on failure
 */
data::any pre_process_if_lob(data::any in, evaluator_context& ctx);

/**
 * @brief Post-process LOB reference returned from functions.
 * @details For LOB references in session storage, registers them to datastore
 * and returns a datastore reference. If the input is not a LOB type or already
 * in datastore, it is returned as-is.
 * @param in the input value (may be LOB or any other type)
 * @param ctx the evaluator context
 * @return the processed value (datastore reference if LOB was in session storage),
 * or error on failure
 */
data::any post_process_if_lob(data::any in, evaluator_context& ctx);

}  // namespace jogasaki::executor::expr
