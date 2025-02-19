/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "read_lob_file.h"

#include <cstddef>
#include <fstream>
#include <string>
#include <string_view>

#include <takatori/util/string_builder.h>

#include <jogasaki/error/error_info_factory.h>

namespace jogasaki::utils {

using takatori::util::string_builder;

status read_lob_file(std::string_view path, std::string& out, std::shared_ptr<error::error_info>& error) {
    std::ifstream fs{std::string{path}, std::ios::binary};
    if (! fs) {
        auto res = status::err_io_error;
        error = create_error_info(error_code::lob_file_io_error,
            string_builder{} << "failed to open file:" << path << string_builder::to_string,
            res);
        return res;
    }
    fs >> out;
    fs.close();
    return status::ok;
}

}  // namespace jogasaki::utils
