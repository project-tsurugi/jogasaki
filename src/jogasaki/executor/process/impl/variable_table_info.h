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
#include <unordered_map>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/graph.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process::impl {

using takatori::util::maybe_shared_ptr;
namespace relation = takatori::relation;

/**
 * @brief value location information
 */
class value_info {
public:
    /**
     * @brief create empty info
     */
    constexpr value_info() = default;

    /**
     * @brief create new object
     * @param value_offset offset of the value
     * @param nullity_offset nullity offset of the value
     */
    constexpr value_info(
        std::size_t value_offset,
        std::size_t nullity_offset
    ) noexcept :
        value_offset_(value_offset),
        nullity_offset_(nullity_offset)
    {}

    /**
     * @brief value offset getter for the value
     * @return value offset
     */
    [[nodiscard]] std::size_t value_offset() const noexcept;

    /**
     * @brief nullity offset getter for the value
     * @return nullity offset
     */
    [[nodiscard]] std::size_t nullity_offset() const noexcept;

private:
    std::size_t value_offset_{};
    std::size_t nullity_offset_{};
};

/**
 * @brief information on variable table
 */
class variable_table_info {
public:
    using variable = takatori::descriptor::variable;
    using entity_type = std::unordered_map<variable, value_info>;
    using variable_indices = std::unordered_map<variable, std::size_t>;
    using named_map_type = std::unordered_map<std::string, value_info>;

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
        entity_type map,
        maybe_shared_ptr<meta::record_meta> meta
    ) noexcept;

    /**
     * @brief construct new instance with variable name support
     * @param indices variable mapping to field index that can be used to retrieve offset from meta
     * @param names the name to variable mapping which provide name mapping support with at(name) function.
     * @param meta metadata of the block variable store
     */
    variable_table_info(
        variable_indices const& indices,
        std::unordered_map<std::string, takatori::descriptor::variable> const& names,
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
     * @brief getter for value location info. for the given variable
     * @param var the variable descriptor
     * @return value_info for the variable
     */
    [[nodiscard]] value_info const& at(variable const& var) const;

    /**
     * @brief getter for value location info. for the given variable
     * @param var the variable descriptor
     * @return value_info for the variable
     */
    [[nodiscard]] value_info const& at(std::string_view name) const;

    /**
     * @brief setter of the name to variable mapping
     * @param name the name of the variable
     * @param var the variable descriptor
     */
    void add(std::string_view name, variable const& var);

    /**
     * @brief returns if the value exists for the given variable
     * @param var the variable descriptor
     * @return true if this object contains the variable
     * @return false otherwise
     */
    [[nodiscard]] bool exists(variable const& var) const;

    /**
     * @brief returns if the value exists for the given variable
     * @param var the variable descriptor
     * @return true if this object contains the variable
     * @return false otherwise
     */
    [[nodiscard]] bool exists(std::string_view name) const;

    /**
     * @brief returns begin iterator for the named variables
     */
    [[nodiscard]] auto name_list_begin() const noexcept {
        return named_map_.begin();
    }

    /**
     * @brief returns end iterator for the named variables
     */
    [[nodiscard]] auto name_list_end() const noexcept {
        return named_map_.end();
    }

    /**
     * @brief accessor to metadata of variable store
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

private:
    entity_type map_{};
    named_map_type named_map_{};
    maybe_shared_ptr<meta::record_meta> meta_{std::make_shared<meta::record_meta>()};
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
[[nodiscard]] std::pair<std::shared_ptr<variables_info_list>, std::shared_ptr<block_indices>>
create_block_variables_definition(
    relation::graph_type const& relations,
    yugawara::compiled_info const& info
);

}


