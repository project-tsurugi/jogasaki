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

#include <string>
#include <string_view>

#include <jogasaki/error_code.h>
#include <jogasaki/status.h>

namespace jogasaki::error {

/**
 * @brief diagnostics object
 * @details this object represents the result details of the API call.
 * If API supports additional diagnostics info, this object can be retrieved just after the API
 * call (e.g. by database::fetch_diagnostics)
 * @warning this api is still evolving and can change frequently in the future
 */
class error_info {
public:
    /**
     * @brief create empty object
     */
    error_info() = default;

    /**
     * @brief destruct the object
     */
    ~error_info() = default;

    error_info(error_info const& other) = default;
    error_info& operator=(error_info const& other) = default;
    error_info(error_info&& other) noexcept = default;
    error_info& operator=(error_info&& other) noexcept = default;

    /**
     * @brief create new object
     */
    explicit error_info(
        error_code code,
        std::string_view message,
        std::string_view filepath,
        std::string_view location,
        std::string_view stacks
    ) noexcept;

    /**
     * @brief accessor to the error message
     * @return the message string
     */
    [[nodiscard]] std::string_view message() const noexcept;

    /**
     * @brief accessor to the error code
     * @return the error code
     */
    [[nodiscard]] jogasaki::error_code code() const noexcept;

    /**
     * @brief set status
     * @deprecated left for compatibility
     */
    void status(jogasaki::status st) noexcept;

    /**
     * @brief accessor to the status
     * @return the status
     * @deprecated left for compatibility
     */
    [[nodiscard]] jogasaki::status status() const noexcept;

    /**
     * @brief fetch source file path
     * @return the source file path
     */
    [[nodiscard]] std::string_view source_file_path() const noexcept;

    /**
     * @brief fetch the position in the source file
     * @return the position in the source file
     */
    [[nodiscard]] std::string_view source_file_position() const noexcept;

    /**
     * @brief fetch error supplemental message
     * @return the supplemental text string
     */
    [[nodiscard]] std::string_view supplemental_text() const noexcept;

private:
    jogasaki::error_code error_code_{};
    std::string message_{};
    jogasaki::status status_{};
    std::string source_file_path_{};
    std::string source_file_position_{};
    std::string stacks_{};

};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, error_info const& value) {
    return out << value.code() << " "
        << value.status() << " "
        << value.message() << " "
        << value.source_file_path() << " "
        << value.source_file_position() << " "
        << value.supplemental_text();
}

}
