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

#include <ostream>

#include <jogasaki/request_context.h>

namespace jogasaki::utils {

/**
 * @brief append request related information (request id, session id, and statement content etc.) to the input stream
 * @param in the output stream to receive the appended info.
 * @param context the request context
 */
void append_request_info(std::ostream& in, request_context& context);

/**
 * @brief print error on log_error level with the info added by append_request_info.
 * @param context the request context
 * @param msg the message
 */
void print_error(request_context& context, std::string_view msg);

} // namespace jogasaki::utils
