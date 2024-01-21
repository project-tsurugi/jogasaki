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

namespace jogasaki::utils {

/**
 * @brief utility class to ensure function call is made when exiting scope like Java finally
 */
class finally {
public:
    using body_type = std::function<void(void)>;

    /**
     * @brief create empty object
     */
    finally() = default;

    finally(finally const& other) = delete;
    finally& operator=(finally const& other) = delete;
    finally(finally&& other) noexcept = delete;
    finally& operator=(finally&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    explicit finally(body_type body) noexcept :
        body_(std::move(body))
    {}

    /**
     * @brief destruct and call the body function
     */
    ~finally() noexcept {
        body_();
    }

private:
    body_type body_{};
};

}  // namespace jogasaki::utils
