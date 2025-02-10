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
#include <ostream>
#include <string>
#include <type_traits>

namespace jogasaki::lob {

/**
 * @brief lob locator object
 * @details Trivially copyable immutable class holding blob reference.
 */
class lob_locator {
public:
    /**
     * @brief default constructor representing empty object
     */
    constexpr lob_locator() = default;

    /**
     * @brief construct new object
     * @param path the lob data file path
     */
    explicit lob_locator(std::string path) :
        path_(std::move(path))
    {}

    /**
     * @brief return path of the blob data file
     */
    [[nodiscard]] std::string_view path() const noexcept {
        return path_;
    }

    /**
     * @brief compare two blob object references
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(lob_locator const& a, lob_locator const& b) noexcept {
        return a.path_ == b.path_;
    }

    /**
     * @brief compare two blob object references
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(lob_locator const& a, lob_locator const& b) noexcept {
        return ! (a == b);
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, lob_locator const& value) {
        return out << "path:" << value.path_;
    }

private:
    std::string path_{};
};

}  // namespace jogasaki
