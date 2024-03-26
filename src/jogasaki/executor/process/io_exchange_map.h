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
#include <vector>

#include <takatori/descriptor/relation.h>

#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>

namespace jogasaki::executor::process {

/**
 * @brief index mapping for input/output exchanges in a process
 * @details a process needs to manage indices (to read from input, write to downstream, write externally)
 * Each index maps to/from exchanges that provides the process with input/output.
 * This object represents the indices for a single process.
 */
class io_exchange_map {
public:
    using relation = takatori::descriptor::relation;
    using input_exchange = executor::exchange::step;
    using output_exchange = executor::exchange::step;
    using external_output_operator = impl::ops::operator_base;

    using input_entity_type = std::vector<input_exchange*>;
    using output_entity_type = std::vector<output_exchange*>;
    using external_output_entity_type = external_output_operator*;

    constexpr static std::size_t npos = static_cast<std::size_t>(-1);
    /**
     * @brief create new empty instance
     */
    io_exchange_map() = default;

    std::size_t add_input(input_exchange* s);
    std::size_t add_output(output_exchange* s);

    void set_external_output(external_output_operator* s);

    std::size_t input_index(input_exchange* s);
    std::size_t output_index(output_exchange* s);

    [[nodiscard]] input_exchange* const& input_at(std::size_t index) const;

    [[nodiscard]] output_exchange* const& output_at(std::size_t index) const;

    [[nodiscard]] external_output_operator const* external_output() const;

    [[nodiscard]] std::size_t input_count() const noexcept;

    [[nodiscard]] std::size_t output_count() const noexcept;

private:
    input_entity_type input_entity_{};
    output_entity_type output_entity_{};
    external_output_entity_type external_output_entity_{};
};

}


