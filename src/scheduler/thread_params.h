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
#include <configuration.h>

namespace jogasaki::scheduler {

class thread_params {
public:
    thread_params() = default;

    explicit thread_params(
            std::size_t threads,
            bool set_core_affinity = true,
            std::size_t initial_core = 1,
            bool assign_nume_nodes_uniformly = false
    ) :
            threads_(threads),
            set_core_affinity_(set_core_affinity),
            initial_core_(initial_core),
            assign_nume_nodes_uniformly_(assign_nume_nodes_uniformly) {}

    explicit thread_params(std::shared_ptr<configuration> cfg) :
            thread_params(cfg->thread_pool_size(), cfg->core_affinity(), cfg->initial_core(), cfg->assign_nume_nodes_uniformly()) {}

    [[nodiscard]] std::size_t threads() const noexcept {
        return threads_;
    };

    [[nodiscard]] bool is_set_core_affinity() const noexcept {
        return set_core_affinity_;
    }

    [[nodiscard]] std::size_t inititial_core() const noexcept {
        return initial_core_;
    }

    [[nodiscard]] bool assign_nume_nodes_uniformly() const noexcept {
        return assign_nume_nodes_uniformly_;
    }

private:
    std::size_t threads_{};
    bool set_core_affinity_{};
    std::size_t initial_core_{};
    bool assign_nume_nodes_uniformly_{};
};

} // namespace
