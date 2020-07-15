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

#include <unordered_map>

#include <takatori/descriptor/relation.h>
#include <jogasaki/executor/exchange/step.h>

namespace jogasaki::plan {

/**
 * @brief map relation descriptor to a exchange step
 */
class relation_step_map {
public:
    using relation = takatori::descriptor::relation;

    using step = executor::exchange::step;
    using entity_type = std::unordered_map<relation, step*>;

    /**
     * @brief create new empty instance
     */
    relation_step_map() = default;

    /**
     * @brief create new instance from map
     */
    explicit relation_step_map(entity_type map) :
        map_(std::move(map))
    {}

    step* const& at(relation const& rel) const {
        return map_.at(rel);
    }

private:
    entity_type map_{};
};

}


