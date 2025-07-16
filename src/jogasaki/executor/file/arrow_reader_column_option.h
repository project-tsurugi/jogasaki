/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <jogasaki/executor/file/time_unit_kind.h>

namespace jogasaki::executor::file::details {

/**
 * @brief column metadata options kept by arrow reader
 */
struct arrow_reader_column_option {

    /**
     * @brief time unit kind for timestamp field
     */
    time_unit_kind time_unit_kind_{time_unit_kind::unspecified};

};

}  // namespace jogasaki::executor::file::details
