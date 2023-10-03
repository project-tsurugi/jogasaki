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

#include <jogasaki/executor/exchange/flow.h>
#include "run_info.h"

namespace jogasaki::executor::exchange::shuffle {

/**
 * @brief flow interface for shuffles
 * @details this provides run info, the step-wide information of run-time data flow processing.
 * The run info should be handled as immutable after transfer().
 */
class flow : public exchange::flow {
public:
    /**
     * @brief create new instance
     */
    flow() = default;

    /**
     * @brief accessor for the run info. This must be used after transfer().
     */
    [[nodiscard]] run_info info() const noexcept;

protected:
    /**
     * @brief accessor to shared run_info source. The update to the run info should be made before/during transfer().
     */
    [[nodiscard]] run_info& updatable_info() noexcept;

    /**
     * @brief inform the completion of transfer so that the run_info is available for read
     */
    void transfer_completed() noexcept;

private:
    run_info info_{};
    bool transfer_completed_{false};
};

}


