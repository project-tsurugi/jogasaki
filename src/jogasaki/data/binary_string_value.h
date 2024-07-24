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

#include <string>

namespace jogasaki::data {

/**
 * @brief binary string value
 * @details the class is essentially the same as std::string, but wrapped so that it can be stored in data::value
 */
class binary_string_value {
public:

    /**
     * @brief construct empty instance
     */
    binary_string_value() = default;

    /**
     * @brief construct new instance
     */
    explicit binary_string_value(std::string arg);

    /**
     * @brief construct new instance
     */
    explicit binary_string_value(std::string_view arg);

    /**
     * @brief fetch body std::string
     */
    [[nodiscard]] std::string const& str() const;

private:
    std::string body_{};
};

}  // namespace jogasaki::data
