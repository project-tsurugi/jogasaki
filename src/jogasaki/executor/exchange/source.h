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

#include <jogasaki/constants.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/executor/io/reader_container.h>

namespace jogasaki::executor::exchange {

class source {
public:
    source() = default;
    virtual ~source() = default;
    source(source const& other) = delete;
    source& operator=(source const& other) = delete;
    source(source&& other) noexcept = delete;
    source& operator=(source&& other) noexcept = delete;

    [[nodiscard]] virtual io::reader_container acquire_reader() = 0;
};

}
