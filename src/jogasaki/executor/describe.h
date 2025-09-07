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

#include <memory>
#include <string_view>

#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/dto/describe_table.h>
#include <jogasaki/request_info.h>
#include <jogasaki/status.h>

namespace jogasaki::executor {

/**
 * @brief describe table
 * @param table_name the target table name
 * @param out the output result, valid only if the return status is status::ok
 * @param error the error info, valid only if the return status is not status::ok
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok if succeeded
 * @return status::err_illegal_operation if the authorization is not sufficient
 * @return any other error status
 */
status describe(
    std::string_view table_name,
    dto::describe_table& out,
    std::shared_ptr<error::error_info>& error,
    request_info const& req_info = {}
);

}  // namespace jogasaki::executor

