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

#include <jogasaki/executor/exchange/flow.h>

namespace jogasaki::executor::exchange::shuffle {

/**
 * @brief the run-time information about the run result of the shuffle
 */
class run_info {
public:

    /**
     * @brief create new instance
     */
    run_info() = default;

    /**
     * @brief destruct the instance
     */
    ~run_info() = default;

    run_info(run_info const& other) = default;
    run_info& operator=(run_info const& other) = default;
    run_info(run_info&& other) noexcept = default;
    run_info& operator=(run_info&& other) noexcept = default;

    /**
     * @brief setter for the empty_input flag, which indicates this data flow object doesn't receive any input record
     */
    void empty_input(bool arg) noexcept;

    /**
     * @brief accessor for the empty_input flag
     */
    [[nodiscard]] bool empty_input() const noexcept;

private:
    bool empty_input_{};
};

}


