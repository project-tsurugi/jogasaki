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

#include <memory>

#include <jogasaki/api/error_info.h>
#include <jogasaki/error/error_info.h>

namespace jogasaki::api::impl {

/**
 * @brief error info object
 * @details this object represents the error information of the API request
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
     * @brief accessor to the error message
     * @return the message string
     */
    [[nodiscard]] std::string_view message() const noexcept override;

    /**
     * @brief accessor to the error code
     * @return the error code
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

    /**
     * @brief factory function for api error info
     * @param body base object
     * @return newly constructed object or nullptr if `body` is nullptr
     */
    static std::shared_ptr<api::impl::error_info> create(std::shared_ptr<error::error_info> body) noexcept;

    /**
     * @brief fetch body
     * @return the body object
     */
    [[nodiscard]] std::shared_ptr<error::error_info> const& body() const noexcept;;
private:
    std::shared_ptr<error::error_info> body_{};

    /**
     * @brief construct new object
     */
    explicit error_info(std::shared_ptr<error::error_info> body) noexcept;

protected:
    void write_to(std::ostream& os) const noexcept override;
};


}

