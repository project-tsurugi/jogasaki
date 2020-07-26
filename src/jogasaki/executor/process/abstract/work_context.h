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

namespace jogasaki::executor::process::abstract {

/**
 * @brief processor working context
 * @details represents transient work area used by the running processor
 */
class work_context {
public:

    /**
     * @brief create new object
     */
    work_context() = default;

    /**
     * @brief destroy this object
     */
    virtual ~work_context() = default;

    work_context(work_context const& other) = delete;
    work_context& operator=(work_context const& other) = delete;
    work_context(work_context&& other) noexcept = delete;
    work_context& operator=(work_context&& other) noexcept = delete;
};

}
