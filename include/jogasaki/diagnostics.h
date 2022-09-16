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

namespace jogasaki {

/**
 * @brief diagnostics object
 * @details this object represents the result details of the API call.
 * If API supports additional diagnostics info, this object can be retrieved just after the API
 * call (e.g. by database::fetch_diagnostics)
 */
class diagnostics {
public:
    /**
     * @brief create empty object
     */
    diagnostics() = default;

    /**
     * @brief destruct the object
     */
    ~diagnostics() = default;

    diagnostics(diagnostics const& other) = default;
    diagnostics& operator=(diagnostics const& other) = default;
    diagnostics(diagnostics&& other) noexcept = default;
    diagnostics& operator=(diagnostics&& other) noexcept = default;

    /**
     * @brief setter for the diagnostics information
     * @param msg the diagnostics message string
     */
    void set(std::string_view msg) noexcept {
        message_ = msg;
    }

    /**
     * @brief accessor to the diagnostics message
     * @return the message string
     */
    [[nodiscard]] std::string_view message() const noexcept {
        return message_;
    }

    /**
     * @brief returns whether the diagnostics contains non-empty information
     * @return true if the diagnostics is not empty
     * @return false otherwise
     */
    [[nodiscard]] operator bool() const noexcept {
        return ! message_.empty();
    }

private:
    std::string message_{};
};

}

