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
#include "worker.h"

#include <cstdint>
#include <variant>
#include <functional>
#include <emmintrin.h>

#include <glog/logging.h>

namespace tateyama::impl {

worker::worker(std::vector<queue>& queues, worker_stat& stat, task_scheduler_cfg const* cfg) noexcept:
    cfg_(cfg),
    queues_(std::addressof(queues)),
    stat_(std::addressof(stat))
{
    (void)cfg_;
}

void worker::operator()(context& ctx) {
    auto index = ctx.index();
    (*queues_)[index].reconstruct();
    auto& q = (*queues_)[index];
    std::size_t last_stolen = index;
    while(q.active()) {
        task_ref t{};
        if (q.try_pop(t)) {
            (*t.body())(ctx);
            ++stat_->count_;
            continue;
        }
        bool stolen = false;
        std::size_t from = last_stolen;
        for(auto idx = next(from, from); idx != from; idx = next(idx, from)) {
            auto& tgt = (*queues_)[idx];
            if(tgt.try_pop(t)) {
                ++stat_->stolen_;
                stolen = true;
                last_stolen = idx;
                DLOG(INFO) << "task stolen from queue " << idx << " to " << index;
                (*t.body())(ctx);
                ++stat_->count_;
                break;
            }
        }
        if (! stolen) {
            _mm_pause();
        }
    }
}

std::size_t worker::next(std::size_t current, std::size_t initial) {
    (void)initial;
    auto sz = queues_->size();
    if (current == sz - 1) {
        return 0;
    }
    return current + 1;
}
}
