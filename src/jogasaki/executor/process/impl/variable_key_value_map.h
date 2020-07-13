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

#include <takatori/descriptor/variable.h>
#include "variable_value_map.h"

namespace jogasaki::executor::process::impl {

class key_value_info {
public:

    [[nodiscard]] bool is_key() const noexcept {
        return is_key_;
    }

    [[nodiscard]] std::size_t value_offset() const noexcept {
        return value_info_.value_offset();
    }

    [[nodiscard]] std::size_t nullity_offset() const noexcept {
        return value_info_.nullity_offset();
    }
private:
    bool is_key_{};
    value_info value_info_{};
};
/**
 * @brief map variables descriptor to key/value location in the record reference
 * @details This map is associated with a single group metadata. Use in pair with a group_meta,
 * that holds fields offset/layout information of the key/value.
 */
class variable_key_value_map {
public:
    using variable = takatori::descriptor::variable;

    using entity_type = std::unordered_map<variable, key_value_info>;

    /**
     * @brief create new empty instance
     */
    variable_key_value_map() = default;

    /**
     * @brief create new instance from map
     */
    explicit variable_key_value_map(entity_type map) :
        map_(std::move(map))
    {}

    key_value_info const& at(variable const& var) const {
        return map_.at(var);
    }

private:
    entity_type map_{};
};

}


