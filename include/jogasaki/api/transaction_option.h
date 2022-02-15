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
#include <memory>

namespace jogasaki::api {

/**
 * @brief transaction option
 * @details this is used to assign values to transaction option
 */
class transaction_option {
public:
    ~transaction_option() = default;
    transaction_option(transaction_option const& other) = default;
    transaction_option& operator=(transaction_option const& other) = default;
    transaction_option(transaction_option&& other) noexcept = default;
    transaction_option& operator=(transaction_option&& other) noexcept = default;

    explicit transaction_option(
        bool readonly = false,
        bool is_long = false,
        std::vector<std::string> write_preserves = {}
    ) :
        readonly_(readonly),
        is_long_(is_long),
        write_preserves_(std::move(write_preserves))
    {}

    [[nodiscard]] bool readonly() const noexcept {
        return readonly_;
    }

    [[nodiscard]] bool is_long() const noexcept {
        return is_long_;
    }

    [[nodiscard]] std::vector<std::string> const& write_preserves() const noexcept {
        return write_preserves_;
    }

private:
    bool readonly_ = false;
    bool is_long_ = false;
    std::vector<std::string> write_preserves_{};
};

}
