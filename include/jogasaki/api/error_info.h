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

#include <string_view>
#include <ostream>

#include <jogasaki/error_code.h>
#include <jogasaki/status.h>

namespace jogasaki::api {

/**
 * @brief error information
 * @details this interface represents a error information of a request result
 */
class error_info {
public:

    /**
     * @brief construct empty object
     */
    error_info() = default;

    /**
     * @brief copy construct
     */
    error_info(error_info const&) = default;

    /**
     * @brief move construct
     */
    error_info(error_info &&) = default;

    /**
     * @brief copy assign
     */
    error_info& operator=(error_info const&) = default;

    /**
     * @brief move assign
     */
    error_info& operator=(error_info &&) = default;

    /**
     * @brief destruct record
     */
    virtual ~error_info() = default;

    /**
     * @brief accessor to the error message
     * @return the message string
     */
    [[nodiscard]] virtual std::string_view message() const noexcept = 0;

    /**
     * @brief accessor to the error code
     * @return the error code
     */
    [[nodiscard]] virtual jogasaki::error_code code() const noexcept = 0;

    /**
     * @brief accessor to the status
     * @return the status
     * @deprecated left for compatibility
     */
    [[nodiscard]] virtual jogasaki::status status() const noexcept = 0;

    /**
     * @brief fetch error supplemental message
     * @return the supplemental text string
     */
    [[nodiscard]] virtual std::string_view supplemental_text() const noexcept = 0;

    /**
     * @brief appends string representation of the error info
     * @param out the target output
     * @param value the target value
     * @return the output stream
     */
    friend inline std::ostream& operator<<(std::ostream& out, error_info const& value) {
        value.write_to(out);
        return out;
    }

protected:
    virtual void write_to(std::ostream& os) const noexcept = 0;
};

}

