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
#include "utils.h"

#include <ostream>
#include <cassert>

#include <tateyama/impl/queue.h>

#include <emmintrin.h>

namespace tateyama::impl {

void spin_wait(std::size_t times, queue& wait) {
    if (times > 0) {
        auto count = times;
        task_ref d{};
        while(--count > 0) {
            if(wait.try_pop(d)) {
                wait.push(std::move(d));  // never come here. Just to avoid optimization.
                return;
            }
            _mm_pause();
        }
    }
}

void measure_spin_wait(std::size_t time, queue& wait) {
    using clock = std::chrono::high_resolution_clock;
    auto begin = clock::now();
    spin_wait(time, wait);
    auto end = clock::now();
    auto duration_ns = std::chrono::duration_cast<clock::duration>(end-begin).count();
    LOG(INFO) << "task_workload : " << time << " took " << duration_ns << "(ns)";
}
}

