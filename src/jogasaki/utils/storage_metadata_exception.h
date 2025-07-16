/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <exception>
#include <jogasaki/status.h>
#include <jogasaki/error_code.h>

namespace jogasaki::utils {

/**
 * @brief storage metadata exception
 * @details The exception thrown on error by storage metadata serializer.
 */
class storage_metadata_exception : public std::exception
{
public:
    /**
     * @brief create empty object
     */
    storage_metadata_exception() = default;

    /**
     * @brief destruct the object
     */
    ~storage_metadata_exception() override = default;

    storage_metadata_exception(storage_metadata_exception const& other) = default;
    storage_metadata_exception& operator=(storage_metadata_exception const& other) = default;
    storage_metadata_exception(storage_metadata_exception&& other) noexcept = default;
    storage_metadata_exception& operator=(storage_metadata_exception&& other) noexcept = default;

    storage_metadata_exception(
        status st,
        error_code code,
        std::string_view msg = {}
    ) noexcept :
        status_(st),
        code_(code),
        msg_(msg)
    {}

    [[nodiscard]] char const* what() const noexcept override {
        return msg_.c_str();
    }

    [[nodiscard]] status get_status() const noexcept {
        return status_;
    }

    [[nodiscard]] error_code get_code() const noexcept {
        return code_;
    }

private:
    status status_{};
    error_code code_{};
    std::string msg_;
};

}
