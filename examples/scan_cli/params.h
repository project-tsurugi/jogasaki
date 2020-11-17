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
#include <cstddef>

namespace jogasaki::scan_cli {

class params {
public:
    std::size_t records_per_partition_ = 1000;
    std::size_t partitions_ = 10;
    bool debug_ = false;
    bool sequential_data_ = false;
    bool randomize_partition_ = true;
    bool dump_ = false;
    bool load_ = false;
    bool no_text_ = false;
};

}
