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

#include <cstddef>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>

#include <jogasaki/status.h>
#include <jogasaki/error/error_info.h>

namespace jogasaki::utils {

/**
 * @brief read lob data from file
 * @param path the path to the lob file
 * @param out the output string
 * @param error the error information filled when other status code than
 * status::ok is returned
 * @return status::ok if success
 * @return any other status otherwise
 */
status read_lob_file(std::string_view path, std::string& out, std::shared_ptr<error::error_info>& error);

}  // namespace jogasaki::utils
