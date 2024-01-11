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
#include "durability_manager.h"

#include <atomic>
#include <memory>
#include <queue>

#include <takatori/util/exception.h>

#include <jogasaki/utils/hex.h>

namespace jogasaki {

using takatori::util::throw_exception;

durability_manager::marker_type durability_manager::current_marker() const {
    if(! current_set_) {
        throw_exception(std::logic_error{""});
    }
    return current_;
}

bool durability_manager::instant_update_if_waitlist_empty(marker_type marker) {
    bool v{false};
    if(! heap_in_use_.compare_exchange_strong(v, true)) {
        // already in use
        return false;
    }
    if(! heap_.empty()) {
        heap_in_use_ = false;
        return false;
    }

    if(! current_set_ || current_ < marker) {
        current_ = marker;
    }
    current_set_ = true;
    heap_in_use_ = false;
    return true;
}

bool durability_manager::update_current_marker(
    marker_type marker,
    callback cb  //NOLINT(performance-unnecessary-value-param)
) {
    bool v{false};
    if(! heap_in_use_.compare_exchange_strong(v, true)) {
        // already in use
        return false;
    }
    element_type top{};
    while(heap_.try_pop(top)) {
        if(top->transaction()->durability_marker() > marker) {
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
    heap_.emplace(std::move(arg));
}

void durability_manager::print_diagnostic(std::ostream& os) {
    auto sz = heap_.size();
    os << "durable_wait_count: " << sz << std::endl;
    if(sz == 0) {
        return;
    }
    std::vector<durability_manager::element_type> backup{};
    backup.reserve(sz);
    durability_manager::element_type elem{};
    while(heap_.try_pop(elem)) {
        backup.emplace_back(std::move(elem));
    }
    os << "durable_waits:" << std::endl;
    for(auto&& e : backup) {
        auto tx = e->transaction();
        os << "  - transaction id: " << (tx ? tx->transaction_id() : "na" ) << std::endl;
        auto job = e->job();
        os << "    job_id: ";
        if(job) {
            os << utils::hex(job->id());
        } else {
            os << "na";
        }
        os << std::endl;
        os << "    marker: ";
        if(! tx || ! tx->durability_marker().has_value()) {
            os << "na";
        } else {
            os << std::to_string(tx->durability_marker().value());
        }
        os << std::endl;
    }

    for(auto&& e : backup) {
        heap_.emplace(std::move(e));
    }
}

}  // namespace jogasaki
