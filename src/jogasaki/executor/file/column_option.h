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

#include <cstdint>

namespace jogasaki::executor::file::details {

/**
 * @brief column metadata options kept by arrow writer
 */
struct column_option {
    /**
     * @brief constant for undefined precision and scale
     */
    constexpr static std::size_t undefined = static_cast<std::size_t>(-1);

    /**
     * @brief length for character field
     * @details this property is mandatory for character field, and undefined for other types
     */
    std::size_t length_{undefined};

    /**
     * @brief varying flag for for character field
     * @details this property is mandatory for character field, and undefined for other types
     */
    bool varying_{false};

    /**
     * @brief precision for decimal field
     * @details this property is mandatory for decimal field, and undefined for other types
     */
    std::size_t precision_{undefined};

    /**
     * @brief scale for decimal field
     * @details this property is mandatory for decimal field, and undefined for other types
     */
    std::size_t scale_{undefined};
};

}  // namespace jogasaki::executor::file::details
