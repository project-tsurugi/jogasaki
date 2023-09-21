/*
 * Copyright 2018-2022 tsurugi project.
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
#include "durability_manager.h"

#include <atomic>
#include <memory>
#include <queue>

#include <takatori/util/exception.h>

namespace jogasaki {

using takatori::util::throw_exception;

durability_manager::durability_marker_type durability_manager::current_durability_marker() const {
    if(! marker_set_) {
        throw_exception(std::logic_error{""});
    }
    return current_;
}

void update_if_larger(
    std::atomic<durability_manager::durability_marker_type>& target,
    durability_manager::durability_marker_type new_value
) {
    auto cur = target.load();
    do {
        if(new_value <= cur) {
            return;
        }
    } while(! target.compare_exchange_strong(cur, new_value));
}

bool durability_manager::update_durability_marker(
    durability_manager::durability_marker_type marker,
    durability_manager::callback cb
) {
    update_if_larger(current_, marker);
    element_type top{};
    while(heap_.try_pop(top)) {
        // Note that current_ can be larger than marker at this point
        // because of other threads calling this function concurrently.
        if(top->durability_marker() > current_) {
            heap_.push(std::move(top));
            break;
        }
        cb(top);
    }
    marker_set_ = true;
    return true;
}

void durability_manager::add(durability_manager::element_type arg) {
    heap_.push(std::move(arg));
}
}

