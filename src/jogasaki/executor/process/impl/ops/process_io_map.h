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
#include <jogasaki/executor/process/impl/ops/operator_base.h>

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief index mapping for input/output exchanges in a process
 * @details a process needs to manage indices (to read from input, write to downstream, write externally)
 * Each index maps to/from exchanges that provides the process with input/output.
 * This object represents the indices for a single process.
 */
class process_io_map {
public:
    using relation = takatori::descriptor::relation;
    using input_step = executor::exchange::step;
    using output_step = executor::exchange::step;
    using external_output_op = operator_base;

    using input_entity_type = std::vector<input_step*>;
    using output_entity_type = std::vector<output_step*>;
    using external_output_entity_type = std::vector<external_output_op*>;

    constexpr static std::size_t npos = static_cast<std::size_t>(-1);
    /**
     * @brief create new empty instance
     */
    process_io_map() = default;

    std::size_t add_input(input_step* s) {
        input_entity_.emplace_back(s);
        return input_entity_.size() - 1;
    }
    std::size_t add_output(output_step* s) {
        output_entity_.emplace_back(s);
        return output_entity_.size() - 1;
    }

    std::size_t add_external_output(external_output_op* s) {
        external_output_entity_.emplace_back(s);
        return external_output_entity_.size() - 1;
    }

    std::size_t input_index(input_step* s) {
        for(std::size_t i=0, n=input_entity_.size(); i < n; ++i) {
            if(input_entity_[i] == s) {
                return i;
            }
        }
        return npos;
    }
    std::size_t output_index(output_step* s) {
        for(std::size_t i=0, n=output_entity_.size(); i < n; ++i) {
            if(output_entity_[i] == s) {
                return i;
            }
        }
        return npos;
    }
    std::size_t external_output_index(external_output_op* s) {
        for(std::size_t i=0, n=external_output_entity_.size(); i < n; ++i) {
            if(external_output_entity_[i] == s) {
                return i;
            }
        }
        return npos;
    }

    [[nodiscard]] input_step* const& input_at(std::size_t index) const {
        return input_entity_.at(index);
    }

    [[nodiscard]] output_step* const& output_at(std::size_t index) const {
        return output_entity_.at(index);
    }

    [[nodiscard]] external_output_op* const& external_output_at(std::size_t index) const {
        return external_output_entity_.at(index);
    }

    [[nodiscard]] std::size_t input_count() const noexcept {
        return input_entity_.size();
    }

    [[nodiscard]] std::size_t output_count() const noexcept {
        return output_entity_.size();
    }

    [[nodiscard]] std::size_t external_output_count() const noexcept {
        return external_output_entity_.size();
    }

private:
    input_entity_type input_entity_{};
    output_entity_type output_entity_{};
    external_output_entity_type external_output_entity_{};
};

}


