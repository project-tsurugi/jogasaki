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

#include <takatori/statement/statement.h>
#include <yugawara/compiled_info.h>

namespace jogasaki::plan {

/**
 * @brief verify the BLOB relay service is available if the statement requires it
 * @details A user defined function passes/receives the LOB data via the BLOB relay service when
 * it has LOB in its parameters or in its return value. Such a statement cannot be executed if
 * the BLOB relay service is unavailable on this server, so detect it on compiling the statement
 * and raise an error rather than failing on the execution.
 * @param statement the compiled statement to be checked
 * @param info the compiled info for the statement
 * @throws plan_exception if the statement requires the BLOB relay service and it's unavailable
 */
void check_blob_relay_availability(
    ::takatori::statement::statement const& statement,
    ::yugawara::compiled_info const& info
);

}  // namespace jogasaki::plan
