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

durability_manager::durability_marker_type durability_manager::current_marker() const {
    if(! current_set_) {
        throw_exception(std::logic_error{""});
    }
    return current_;
}

bool durability_manager::update_current_marker(
    durability_marker_type marker,
    callback cb  //NOLINT(performance-unnecessary-value-param)
) {
    if(heap_in_use_) {
        return false;
    }
    heap_in_use_ = true;
    element_type top{};
    while(heap_.try_pop(top)) {
        if(top->durability_marker() > marker) {
            heap_.push(std::move(top));
            break;
        }
        cb(top);
    }
    if(! current_set_ || current_ < marker) {
        current_ = marker;
    }
    current_set_ = true;
    heap_in_use_ = false;
    return true;
}

void durability_manager::add_to_waitlist(durability_manager::element_type arg) {
    heap_.push(std::move(arg));
}
}

