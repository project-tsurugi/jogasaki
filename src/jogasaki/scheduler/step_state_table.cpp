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
#include "step_state_table.h"

#include <algorithm>

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/scheduler/step_state.h>

namespace jogasaki::scheduler {

void step_state_table::assign_slot(step_state_table::kind k, std::size_t n) {
    if (k == kind::main) {
        main_slots_.resize(main_slots_.size() + n, uninitialized_task_identity);
    } else {
        sub_slots_.resize(sub_slots_.size() + n, uninitialized_task_identity);
    }
}

std::size_t step_state_table::slots(step_state_table::kind k) const {
    return k == kind::main ? main_slots_.size() : sub_slots_.size();
}

std::vector<step_state_table::slot_index> step_state_table::list_uninitialized(step_state_table::kind k) const {
    return list_uninitialized(k == kind::main ? main_slots_ : sub_slots_);
}

bool step_state_table::uninitialized_slot(step_state_table::kind k, step_state_table::slot_index ind) const {
    return uninitialized_slot(k == kind::main ? main_slots_ : sub_slots_, ind);
}

void step_state_table::register_task(step_state_table::kind k, step_state_table::slot_index slot,
    model::task::identity_type id) {
    register_task(k == kind::main ? main_slots_ : sub_slots_, slot, id);
}

step_state_table::kind step_state_table::task_state(model::task::identity_type id, task_state_kind st) {
    auto it = std::find(main_slots_.begin(), main_slots_.end(), id);
    if (it != main_slots_.end()) {
        main_status_[*it] = st;
        return kind::main;
    }
    it = std::find(sub_slots_.begin(), sub_slots_.end(), id);
    if (it != sub_slots_.end()) {
        sub_status_[*it] = st;
        return kind::pre;
    }
    throw std::domain_error("invalid identity");
}

bool step_state_table::completed(step_state_table::kind k) const {
    return k == kind::main ?
        tasks_completed(main_status_, main_slots_.size()) :
        tasks_completed(sub_status_, sub_slots_.size());
}

bool step_state_table::tasks_completed(const step_state_table::entity_type &status, std::size_t count) const {
    if (status.size() != count) {
        return false; //NOLINT //bug of clang-tidy?
    }
    for(auto& p : status) {
        if (p.second != task_state_kind::completed) {
            return false;
        }
    }
    return true;
}

void step_state_table::register_task(step_state_table::slots_type &slots, step_state_table::slot_index slot,
    model::task::identity_type id) {
    if (slot >= slots.size()) {
        throw std::domain_error("insufficient slots");
    }
    slots[slot] = id;
}

std::vector<step_state_table::slot_index> step_state_table::list_uninitialized(const step_state_table::slots_type &slots) const {
    std::vector<slot_index> v{};
    v.reserve(slots.size());
    for(slot_index i = 0; i < slots.size(); ++i) {
        if (slots[i] == uninitialized_task_identity) {
            v.emplace_back(i);
        }
    }
    return v;
}

bool
step_state_table::uninitialized_slot(const step_state_table::slots_type &slots, step_state_table::slot_index ind) const {
    if (ind >= slots.size()) {
        return true;
    }
    return slots[ind] == uninitialized_task_identity;
}
}
