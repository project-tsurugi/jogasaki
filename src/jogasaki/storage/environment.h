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

#include <sharksfin/Environment.h>

namespace jogasaki::storage {

/**
 * @brief context for the transactional storage engine
 */
class environment {
public:
    /**
     * @brief create default context object
     */
    environment() = default;

    /**
     * @brief create default context object
     */
    ~environment() = default;

    // TODO other constructors

    void initialize() {
        environment_.initialize();
    }
private:
    sharksfin::Environment environment_{};
};

}

