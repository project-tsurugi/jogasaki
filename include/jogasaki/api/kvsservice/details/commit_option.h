/*
 * Copyright 2018-2023 tsurugi project.
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

#include <cstdint>

namespace jogasaki::api::kvsservice::details {

/**
 * @brief commit operation behavior
 */
enum class commit_option : std::uint32_t {
    /**
     * @brief the default commit type (rely on the database settings).
     */
    commit_type_unspecified = 0,

    /**
     * @brief wait unti the commit operation has accepted.
     */
    accepted = 10,

    /**
     * @brief wait until the commit data have been visible for others.
     */
    available = 20,

    /**
     * @brief wait until the commit data have been saved on the local disk.
     */
    stored = 30,

    /**
     * @brief wait until the commit data have been propagated to the all suitable replicas.
     */
    propagated = 40,
};
}
