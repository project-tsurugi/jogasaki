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

#include <jogasaki/status.h>
#include <jogasaki/error/code.h>

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
        code error_code,
        std::string_view message = {}
    ) noexcept :
        error_code_(error_code),
        message_(message)
    {}

    /**
     * @brief setter for the error_info information
     * @param msg the error_info message string
     */
    void set(std::string_view msg) noexcept {
        message_ = msg;
    }

    /**
     * @brief accessor to the error_info message
     * @return the message string
     */
    [[nodiscard]] std::string_view message() const noexcept {
        return message_;
    }

    /**
     * @brief accessor to the error_info message
     * @return the message string
     */
    [[nodiscard]] code error_code() const noexcept {
        return error_code_;
    }

    /**
     * @brief returns whether the error_info contains non-empty information
     * @return true if the error_info is not empty
     * @return false otherwise
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return ! message_.empty();
    }

    /**
     * @brief clear and reset the error_info content
     */
    void clear() noexcept {
        message_.clear();
    }

    /**
     * @brief set status
     * @deprecated left for compatibility
     */
    void status(jogasaki::status st) noexcept {
        status_ = st;
    }

    /**
     * @brief accessor to the status
     * @return the status
     * @deprecated left for compatibility
     */
    [[nodiscard]] jogasaki::status status() const noexcept {
        return status_;
    }
private:
    code error_code_{};
    std::string message_{};
    jogasaki::status status_{};
};

}

