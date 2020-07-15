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

#include <cassert>

#include <takatori/relation/expression.h>
#include <takatori/descriptor/variable.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/util/fail.h>
#include <yugawara/analyzer/block_builder.h>
#include <yugawara/analyzer/block_algorithm.h>
#include <yugawara/analyzer/variable_liveness_analyzer.h>
#include <yugawara/compiled_info.h>
#include <yugawara/compiler_result.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/relop/operator_base.h>
#include <jogasaki/executor/process/impl/block_variables_info.h>

namespace jogasaki::meta {

using takatori::util::fail;
using takatori::descriptor::variable;

enum class variable_ordering_kind {
    ///@brief flat record (for forward exchange)
    flat_record,

    ///@brief construct record from keys and values (scan operator)
    flat_record_from_keys_values,

    ///@brief construct group meta from keys selected from values (group exchange)
    group_from_keys,
};

template<auto Kind>
struct variable_ordering_enum_tag_t {
    explicit variable_ordering_enum_tag_t() = default;
};

template<auto Kind>
inline constexpr variable_ordering_enum_tag_t<Kind> variable_ordering_enum_tag{};

/**
 * @brief represents variables order in a relation (e.g. exchange, index, or table)
 *
 * @details This is the central place to collectively handle all rules to determine the columns order.
 * The engine defines variables order based on ordering kind provided to the constructor.
 * This class objects represents the order by providing map from variables to 0-origin index.
 */
class variable_order {
public:
    using variable_index_type = std::size_t;
    using entity_type = std::unordered_map<variable, variable_index_type>;
    using key_bool_type = std::unordered_map<variable, bool>;

    [[nodiscard]] variable_index_type index(variable const& var) const {
        return entity_.at(var);
    }

    [[nodiscard]] std::pair<variable_index_type, bool> key_value_index(variable const& var) const {
        assert(for_group_);  //NOLINT
        return { entity_.at(var), key_bool_.at(var) };
    }

    [[nodiscard]] bool for_group() const noexcept {
        return for_group_;
    }

    [[nodiscard]] bool is_key(variable const& var) const {
        assert(for_group_);  //NOLINT
        return key_bool_.at(var);
    }

    variable_order() = default;

    void fill_flat_record(
        entity_type& entity,
        std::vector<variable, takatori::util::object_allocator<variable>> const& columns,
        std::size_t begin_offset = 0
    ) {
        // oredering arbitrarily for now
        //TODO order shorter types first, and alphabetically
        entity.reserve(columns.size());
        for(std::size_t i=0, n =columns.size(); i < n; ++i) {
            entity.emplace(columns[i], i+begin_offset);
        }
    }

    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::flat_record>,
        std::vector<variable, takatori::util::object_allocator<variable>> const& columns
    ) {
        fill_flat_record(entity_, columns);
    }

    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::flat_record_from_keys_values>,
        std::vector<variable, takatori::util::object_allocator<variable>> const& keys,
        std::vector<variable, takatori::util::object_allocator<variable>> const& values
    ) {
        entity_type keys_order{};
        entity_type values_order{};
        fill_flat_record(keys_order, keys);
        fill_flat_record(values_order, values, keys.size());
        entity_.merge(keys_order);
        entity_.merge(values_order);
    }

    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::group_from_keys>,
        std::vector<variable, takatori::util::object_allocator<variable>> const& columns,
        std::vector<variable, takatori::util::object_allocator<variable>> const& group_keys
    ) : for_group_{true}
    {
        entity_type keys_order{};
        fill_flat_record(keys_order, group_keys);

        std::vector<variable, takatori::util::object_allocator<variable>> values{};
        values.reserve(columns.size() - group_keys.size());
        for(auto&& column : columns) {
            if (keys_order.count(column) == 0) {
                values.emplace_back(column);
            }
        }
        entity_type values_order{};
        fill_flat_record(values_order, values);

        entity_.reserve(columns.size());
        for(auto&& k : group_keys) {
            entity_.emplace(k, keys_order[k]);
            key_bool_.emplace(k, true);
        }
        for(auto&& v : values) {
            entity_.emplace(v, values_order[v]);
            key_bool_.emplace(v, false);
        }
    }

private:
    entity_type entity_;
    key_bool_type key_bool_{};
    bool for_group_{false};
};

}

