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

#include <takatori/graph/graph.h>
#include <takatori/plan/graph.h>
#include <yugawara/compiler_result.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/plan/parameter_set.h>

namespace jogasaki::executor::process {

namespace relation = takatori::relation;
using executor::process::impl::variable_table;

using takatori::util::maybe_shared_ptr;

class processor_details {
public:
    processor_details() = default;

    processor_details(
        bool has_scan_operator,
        bool has_emit_operator,
        bool has_find_operator,
        bool has_write_operations
    );

    [[nodiscard]] bool has_scan_operator() const noexcept;
    [[nodiscard]] bool has_emit_operator() const noexcept;
    [[nodiscard]] bool has_find_operator() const noexcept;
    [[nodiscard]] bool has_write_operations() const noexcept;

private:
    bool has_scan_operator_ = false;
    bool has_emit_operator_ = false;
    bool has_find_operator_ = false;
    bool has_write_operations_ = false;
};

/**
 * @brief processor specification packing up all compile-time (takatori/yugawara) information
 * necessary for the processor to run.
 *
 * This object contains only compile time information and derived objects such as jogasaki operators
 * are not part of this info.
 */
class processor_info {
public:
    processor_info() = default;

    processor_info(
        relation::graph_type const& relations,
        yugawara::compiled_info info,
        maybe_shared_ptr<impl::variables_info_list const> vars_info_list,
        maybe_shared_ptr<impl::block_indices const> block_inds,
        variable_table const* host_variables = nullptr
    );

    processor_info(
        relation::graph_type const& relations,
        yugawara::compiled_info info,
        variable_table const* host_variables = nullptr
    );

    [[nodiscard]] relation::graph_type const& relations() const noexcept;

    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept;

    [[nodiscard]] impl::variables_info_list const& vars_info_list() const noexcept;

    [[nodiscard]] impl::block_indices const& block_indices() const noexcept;

    [[nodiscard]] processor_details const& details() const noexcept;

    [[nodiscard]] variable_table const* host_variables() const noexcept {
        return host_variables_;
    }

private:
    relation::graph_type const* relations_{};
    yugawara::compiled_info info_{};
    maybe_shared_ptr<impl::variables_info_list const> vars_info_list_{};
    maybe_shared_ptr<impl::block_indices const> block_indices_{};
    processor_details details_{};
    variable_table const* host_variables_{};

    processor_details create_details();
};

}


