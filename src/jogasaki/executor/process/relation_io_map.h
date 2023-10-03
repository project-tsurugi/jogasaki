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

#include <unordered_map>

#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process {

class relation_io_map {
public:
    using entity_type = std::unordered_map<takatori::descriptor::relation, std::size_t>;

    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create new empty instance
     */
    relation_io_map() = default;

    /**
     * @brief create new instance
     */
    relation_io_map(
        entity_type input_entity,
        entity_type output_entity
    );

    [[nodiscard]] std::size_t input_index(takatori::descriptor::relation const& arg) const;

    [[nodiscard]] std::size_t output_index(takatori::descriptor::relation const& arg) const;

    [[nodiscard]] std::size_t input_count() const noexcept;

    [[nodiscard]] std::size_t output_count() const noexcept;
private:
    entity_type input_entity_{};
    entity_type output_entity_{};
};

}


