/*
 * Copyright 2018-2021 tsurugi project.
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

#include <cstddef>
#include <tateyama/status.h>

namespace tateyama::api::endpoint {

/**
 * @brief buffer interface
 */
class buffer {
public:
    /**
     * @brief create empty object
     */
    buffer() = default;

    /**
     * @brief destruct the object
     */
    virtual ~buffer() = default;

    buffer(buffer const& other) = default;
    buffer& operator=(buffer const& other) = default;
    buffer(buffer&& other) noexcept = default;
    buffer& operator=(buffer&& other) noexcept = default;

    /**
     * @brief return the pointer to the data area owned by this object
     * @return the pointer to the data area
     */
    virtual status write(char const* data, std::size_t length) = 0;
};

}
