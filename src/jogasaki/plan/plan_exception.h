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

#include <exception>
#include <memory>
#include <string_view>
#include <utility>

#include <jogasaki/error/error_info.h>
#include <jogasaki/status.h>

namespace jogasaki::plan {

/**
 * @brief exception for plan generation
 * @details this exception is thrown when the plan generation
 * (compile by sql compiler and constructing parallel plan objects by jogasaki) fails
 */
class plan_exception : public std::exception {
public:
    /**
     * @brief create empty object
     */
    plan_exception() = default;

    /**
     * @brief destruct the object
     */
    ~plan_exception() override = default;

    plan_exception(plan_exception const& other) = default;
    plan_exception& operator=(plan_exception const& other) = default;
    plan_exception(plan_exception&& other) noexcept = default;
    plan_exception& operator=(plan_exception&& other) noexcept = default;

    explicit plan_exception(std::shared_ptr<error::error_info> info) noexcept :
        info_(std::move(info))
    {}

    [[nodiscard]] char const* what() const noexcept override {
        if(! info_) return "";
        return info_->message().data();
    }

    [[nodiscard]] std::shared_ptr<error::error_info> const& info() const noexcept {
        return info_;
    }

private:
    std::shared_ptr<error::error_info> info_{};
};

}  // namespace jogasaki::plan
