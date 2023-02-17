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

#include <cstring>

namespace jogasaki::utils {

static std::size_t basename_start(char const* path) {
    auto* base = std::strrchr(path, '/');
    return base ? (base - path + 1) : 0;
}

}

#define stringify_(t) #t
#define stringify__(t) stringify_(t)

/**
 * @brief base file name string with line number (e.g. `file.cpp:10`)
 */
#define basename_(name)  ((name ":" stringify__(__LINE__)) + jogasaki::utils::basename_start(name))
#define base_filename()  (basename_(__FILE__))

