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

#include <cstdint>
#include <variant>
#include <ios>
#include <functional>
#include <emmintrin.h>

#include <glog/logging.h>

#include <tateyama/context.h>
#include <tateyama/impl/queue.h>
#include <tateyama/impl/thread_control.h>
#include <tateyama/task_scheduler_cfg.h>
#include <tateyama/impl/core_affinity.h>
#include <tateyama/impl/utils.h>

namespace tateyama::impl {

struct cache_align worker_stat {
    std::size_t count_{};
    std::size_t stolen_{};
};

/**
 * @brief worker object
 * @details this represents the worker logic running on each thread that processes its local queue
 */
template <class T>
class cache_align worker {
public:
    using task = T;

    /**
     * @brief create empty object
     */
    worker() = default;

    /**
     * @brief create new object
     * @param queues reference to the queues
     * @param stat worker stat information
     * @param cfg the scheduler configuration information
     */
    worker(
        std::vector<basic_queue<task>>& queues,
        std::vector<std::vector<task>>& initial_tasks,
        worker_stat& stat,
        task_scheduler_cfg const* cfg = nullptr
    ) noexcept:
        cfg_(cfg),
        queues_(std::addressof(queues)),
        initial_tasks_(std::addressof(initial_tasks)),
        stat_(std::addressof(stat))
    {
        (void)cfg_;
    }

    /**
     * @brief the worker body
     * @param ctx the worker context information
     */
    void operator()(context& ctx) {
        auto index = ctx.index();
        (*queues_)[index].reconstruct();
        auto& q = (*queues_)[index];
        auto& s = (*initial_tasks_)[index];
        for(auto&& t : s) {
            q.push(std::move(t));
        }
        s.clear();
        std::size_t last_stolen = index;
        while(q.active()) {
            task t{};
            if (q.try_pop(t)) {
                t(ctx);
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
                    t(ctx);
                    ++stat_->count_;
                    break;
                }
            }
            if (! stolen) {
                _mm_pause();
            }
        }
    }

private:
    task_scheduler_cfg const* cfg_{};
    std::vector<basic_queue<task>>* queues_{};
    std::vector<std::vector<task>>* initial_tasks_{};
    worker_stat* stat_{};

    std::size_t next(std::size_t current, std::size_t initial) {
        (void)initial;
        auto sz = queues_->size();
        if (current == sz - 1) {
            return 0;
        }
        return current + 1;
    }
};

}
