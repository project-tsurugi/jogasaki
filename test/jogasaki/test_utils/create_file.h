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

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki {

inline void create_file(std::string_view path, std::string_view content) {
    std::ofstream fs{std::string{path}, std::ios::binary};
    fs << content;
    fs.close();
}

inline std::string read_file(std::string_view path) {
    std::ifstream fs{std::string{path}, std::ios::binary};
    std::string ret{};
    fs >> ret;
    fs.close();
    return ret;
}

}  // namespace jogasaki
