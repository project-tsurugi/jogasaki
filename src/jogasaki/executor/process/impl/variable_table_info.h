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

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/graph.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/meta/record_meta.h>
#include "variable_value_map.h"

namespace jogasaki::executor::process::impl {

using takatori::util::maybe_shared_ptr;
namespace relation = takatori::relation;

/**
 * @brief information on variable table
 */
class variable_table_info {
public:
    using variable = takatori::descriptor::variable;
    using variable_indices = std::unordered_map<variable, std::size_t>;

    /**
     * @brief construct empty instance
     */
    variable_table_info() = default;

    /**
     * @brief construct new instance
     * @param value_map variable mapping to value offset info. in the store
     * @param meta metadata of the block variable store
     * @attention offset retrieved from value_map and meta should be identical if they correspond to the same variable.
     * Constructor below is more convenient if meta and variable indices are available.
     */
    variable_table_info(
        variable_value_map value_map,
        maybe_shared_ptr<meta::record_meta> meta
    ) noexcept;

    /**
     * @brief construct new instance
     * @param indices variable mapping to field index that can be used to retrieve offset from meta
     * @param meta metadata of the block variable store
     */
    variable_table_info(
        variable_indices const& indices,
        maybe_shared_ptr<meta::record_meta> meta
    ) noexcept;

    /**
     * @brief accessor to variable value map
     */
    [[nodiscard]] variable_value_map const& value_map() const noexcept;

    /**
     * @brief accessor to metadata of variable store
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

private:
    variable_value_map value_map_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

using variables_info_list = std::vector<variable_table_info>;
using block_indices = std::unordered_map<relation::expression const*, std::size_t>;

/**
 * @brief create block related information about the operators in a process
 * @param relations relational operator graph in one process
 * @param info compiled info for the process
 * @return pair of info objects. First object is the list of variable table info object ordered
 * by block index, and second is the mapping from relational operator to the block index.
 */
[[nodiscard]] std::pair<variables_info_list, block_indices> create_block_variables_definition(
    relation::graph_type const& relations,
    yugawara::compiled_info const& info
);

}


