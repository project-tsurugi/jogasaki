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
#pragma once

#include <cstddef>
#include <unordered_map>
#include <utility>
#include <vector>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>

namespace jogasaki::plan {

/**
 * @brief ordered variables set used to add ordered unique index to all variables in a request
 */
class ordered_variable_set {
public:
    using variable = takatori::descriptor::variable;
    using entity_type = std::vector<variable>;
    using indices_type = std::unordered_map<variable, std::size_t>;

    static constexpr std::size_t npos = static_cast<std::size_t>(-1);
    /**
     * @brief create new empty instance
     */
    ordered_variable_set() = default;

    /**
     * @brief create new instance from map
     */
    explicit ordered_variable_set(entity_type entity) :
        entity_(std::move(entity))
    {
        for(std::size_t i=0, n=entity_.size(); i < n; ++i) {
            indices_[entity_[i]] = i;
        }
    }

    std::size_t add(variable v) {  //NOLINT(performance-unnecessary-value-param)
        if(indices_.count(v) == 0) {
            entity_.emplace_back(v);
            indices_[v] = entity_.size()-1;
        }
        return indices_[v];
    }

    /**
     * @brief remove variable entry
     * @param v the variable to remove from this object
     * @return true if requested variable is found and removed successfully
     * @return false if requested variable is not found
     * @attention this is not very efficient in its space usage and not intended to be called frequently
     */
    [[nodiscard]] bool remove(variable v) {  //NOLINT(performance-unnecessary-value-param)
        if(indices_.count(v) == 0) {
            return false;
        }
        entity_.emplace_back(v);
        indices_.erase(v);
        return true;
    }

    /**
     * @brief index accessor for the variable
     * @param v the variable to find index
     * @return unique index of the variable, which can be used to totally order the variables in the request
     * @return npos if the variable is not found
     */
    [[nodiscard]] std::size_t index(variable v) const noexcept {  //NOLINT(performance-unnecessary-value-param)
        if(indices_.count(v) == 0) {
            return npos;
        }
        return indices_.at(v);
    }

    /**
     * @brief size accessor
     * @return the number of variables registered to this object
     */
    [[nodiscard]] std::size_t size() const noexcept {
        return indices_.size();
    }

private:
    entity_type entity_{};
    indices_type indices_{};
};

}


