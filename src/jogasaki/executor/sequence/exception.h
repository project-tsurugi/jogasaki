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

namespace jogasaki::executor::sequence {

/**
 * @brief sequence handling exception
 * @details The exception thrown on unrecoverable error by the objects in jogasaki::executor::sequence.
 * To avoid complex error handling with cc engine, jogasaki::executor::sequence uses this exception on unrecoverable
 * error situations such as occ error or ltx wp error reading/writing system sequence table.
 */
class exception : public std::exception
{
public:
    /**
     * @brief create empty object
     */
    exception() = default;

    /**
     * @brief destruct the object
     */
    ~exception() override = default;

    exception(exception const& other) = default;
    exception& operator=(exception const& other) = default;
    exception(exception&& other) noexcept = default;
    exception& operator=(exception&& other) noexcept = default;

    explicit exception(status st, std::string_view msg = {}) noexcept :
        status_(st),
        msg_(msg)
    {}

    [[nodiscard]] char const* what() const noexcept override {
        return msg_.c_str();
    }

    [[nodiscard]] status get_status() const noexcept {
        return status_;
    }

private:
    status status_{};
    std::string msg_;
};

}  // namespace jogasaki::executor::sequence
