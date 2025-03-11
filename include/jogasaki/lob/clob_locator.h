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

#pragma once

#include <jogasaki/lob/lob_locator.h>

namespace jogasaki::lob {

/**
 * @brief clob locator object
 * @details immutable class holding information to locate lob data
 */
class clob_locator : public lob_locator {
public:
    /**
     * @brief default constructor representing empty object
     */
    constexpr clob_locator() = default;

    /**
     * @brief construct new object
     * @param path the lob data file path
     * @param is_temporary whether the data file on the path is temporary
     */
    clob_locator(std::string path, bool is_temporary) :
        lob_locator(std::move(path), is_temporary)
    {}

};

}  // namespace jogasaki::lob
