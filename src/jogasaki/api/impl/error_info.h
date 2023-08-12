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

#include <memory>

#include <jogasaki/api/error_info.h>
#include <jogasaki/error/error_info.h>

namespace jogasaki::api::impl {

/**
 * @brief record object in the result set
 * @details this interface represents a record in the query result and provides accessor to field values
 */
class error_info : public api::error_info {
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
    ~error_info() override = default;

    /**
     * @brief construct empty object
     */
    explicit error_info(std::shared_ptr<error::error_info> body) noexcept;

    /**
     * @brief accessor to the error_info message
     * @return the message string
     */
    [[nodiscard]] std::string_view message() const noexcept override;

    /**
     * @brief accessor to the error_info message
     * @return the message string
     */
    [[nodiscard]] jogasaki::error_code code() const noexcept override;

    /**
     * @brief accessor to the status
     * @return the status
     * @deprecated left for compatibility
     */
    [[nodiscard]] jogasaki::status status() const noexcept override;

    /**
     * @brief fetch error supplemental message
     * @return the supplemental text string
     */
    [[nodiscard]] std::string_view supplemental_text() const noexcept override;

private:
    std::shared_ptr<error::error_info> body_{};

protected:
    void write_to(std::ostream& os) const noexcept;
};

}

