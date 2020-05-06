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

namespace jogasaki {

class configuration {
public:
    bool single_thread_task_scheduler_ = true;
    std::size_t thread_pool_size_ = 5;
    std::size_t default_process_partitions_ = 5;
    std::size_t default_scan_process_partitions_ = 5;
    bool set_core_affinity_ = false;
    std::size_t initial_core_ = 1;
};

static constexpr configuration default_configuration{};

}

