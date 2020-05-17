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

#include <utils/watch.h>

namespace jogasaki::group_cli {

class params {
public:
    bool use_multithread = true;

    std::size_t thread_pool_size_ = 1;

    std::size_t records_per_upstream_partition_ = 1000;

    std::size_t upstream_partitions_ = 10;

    std::size_t downstream_partitions_ = 10;

    bool set_core_affinity_ = false;

    std::size_t initial_core_ = 1;

    std::shared_ptr<utils::watch> watch_ = std::make_shared<utils::watch>();

};

}
