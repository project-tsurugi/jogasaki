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
#pragma once

#include <takatori/statement/details/table_privilege_element.h>

#include <jogasaki/request_context.h>

namespace jogasaki::executor::common {

/**
 * @brief common function to process GRANT and REVOKE statements
 * @param grant true for GRANT, false for REVOKE
 * @param context the request context
 * @param elements the table privilege elements to process
 * @return true if the operation is successful, false otherwise
 */
bool process_grant_revoke(
    bool grant,
    request_context& context,
    std::vector<takatori::statement::details::table_privilege_element> const& elements
);

}
