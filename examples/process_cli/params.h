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

namespace jogasaki::process_cli {

class params {
public:
    std::size_t records_per_partition_ = 1000;

    std::size_t partitions_ = 10;

    std::size_t write_buffer_size_ = 2*1024*1024;

    std::size_t read_buffer_size_ = 2*1024*1024;

    bool std_allocator = false;

    bool sequential_data = false;
};

}
