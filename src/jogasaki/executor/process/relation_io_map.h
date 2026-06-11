/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/relation.h>
#include <takatori/relation/step/offer.h>

#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process {

/**
 * @brief mapping from relation to input/output indices used by a process
 * @details inputs are looked up by the relation descriptor of the upstream exchange (a process
 * has at most one `take_*` operator per upstream exchange). Outputs are looked up by the `offer`
 * operator instance itself (not by its destination relation), because a single process can
 * contain multiple `offer` operators that target the same downstream exchange,
 * and each such `offer` must be assigned its own output index.
 * The input/output indices are comonly used by `io_exchange_map` and `io_info` to refer the exchanges.
 */
class relation_io_map {
public:
    using input_entity_type = std::unordered_map<takatori::descriptor::relation, std::size_t>;
    using output_entity_type = std::unordered_map<takatori::relation::step::offer const*, std::size_t>;

    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create new empty instance
     */
    relation_io_map() = default;

    /**
     * @brief create new instance
     */
    relation_io_map(
        input_entity_type input_entity,
        output_entity_type output_entity
    );

    [[nodiscard]] std::size_t input_index(takatori::descriptor::relation const& arg) const;

    [[nodiscard]] std::size_t output_index(takatori::relation::step::offer const& arg) const;

    [[nodiscard]] std::size_t input_count() const noexcept;

    [[nodiscard]] std::size_t output_count() const noexcept;
private:
    input_entity_type input_entity_{};
    output_entity_type output_entity_{};
};

}  // namespace jogasaki::executor::process
