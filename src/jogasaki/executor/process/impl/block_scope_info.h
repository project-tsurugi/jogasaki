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

#include <memory>

#include <takatori/relation/expression.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/meta/record_meta.h>
#include <takatori/relation/graph.h>
#include "variable_value_map.h"

namespace jogasaki::executor::process::impl {

namespace relation = takatori::relation;

/**
 * @brief information on block scoped variables
 */
class block_scope_info {
public:
    block_scope_info() = default;

    block_scope_info(
        std::unique_ptr<variable_value_map> value_map,
        std::shared_ptr<meta::record_meta> meta
    ) noexcept;

    [[nodiscard]] variable_value_map& value_map() const noexcept;

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& meta() const noexcept;

private:
    std::unique_ptr<variable_value_map> value_map_{};
    std::shared_ptr<meta::record_meta> meta_{};
};

using scopes_info = std::vector<class block_scope_info>;

using scope_indices = std::unordered_map<takatori::relation::expression const*, std::size_t>;

/**
 * @brief create block related information about the operators in a process
 */
[[nodiscard]] std::pair<scopes_info, scope_indices> create_scopes_info(
    relation::graph_type& relations,
    yugawara::compiled_info const& info);

}


