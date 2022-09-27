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

#include <optional>

namespace jogasaki::api {

/**
 * @brief type information for a time_of_day field
 */
class time_of_day_field_option {
public:

    /**
     * @brief construct empty object
     */
    constexpr time_of_day_field_option() noexcept = default;

    /**
     * @brief construct new object
     */
    explicit constexpr time_of_day_field_option(bool with_offset) noexcept :
        with_offset_(with_offset)
    {}

    /**
     * @brief destruct the object
     */
    ~time_of_day_field_option() = default;

    time_of_day_field_option(time_of_day_field_option const& other) = default;
    time_of_day_field_option& operator=(time_of_day_field_option const& other) = default;
    time_of_day_field_option(time_of_day_field_option&& other) noexcept = default;
    time_of_day_field_option& operator=(time_of_day_field_option&& other) noexcept = default;

    [[nodiscard]] bool with_offset() const noexcept {
        return with_offset_;
    }

private:
    bool with_offset_{};

};

} // namespace

