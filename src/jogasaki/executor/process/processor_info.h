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
#include <jogasaki/executor/process/impl/block_scope_info.h>

namespace jogasaki::executor::process {

namespace relation = takatori::relation;

class processor_details {
public:
    processor_details() = default;

    processor_details(
        bool has_scan_operator,
        bool has_emit_operator,
        bool has_write_operations
    ) :
        has_scan_operator_(has_scan_operator),
        has_emit_operator_(has_emit_operator),
        has_write_operations_(has_write_operations)
    {}

    [[nodiscard]] bool has_scan_operator() const noexcept {
        return has_scan_operator_;
    }
    [[nodiscard]] bool has_emit_operator() const noexcept {
        return has_emit_operator_;
    }
    [[nodiscard]] bool has_write_operations() const noexcept {
        return has_write_operations_;
    }
private:
    bool has_scan_operator_ = false;
    bool has_emit_operator_ = false;
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
        relation::graph_type& relations,
        yugawara::compiled_info const& info
    ) :
        relations_(std::addressof(relations)),
        info_(std::addressof(info)),
        details_(create_details())
    {
        auto&& p = impl::create_scopes_info(*relations_, *info_);
        scopes_info_ = std::move(p.first);
        scope_indices_ = std::move(p.second);
    }

    [[nodiscard]] relation::graph_type& relations() const noexcept {
        return *relations_;
    }

    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept {
        return *info_;
    }

    [[nodiscard]] impl::scopes_info const& scopes_info() const noexcept {
        return scopes_info_;
    }

    [[nodiscard]] impl::scope_indices const& scope_indices() const noexcept {
        return scope_indices_;
    }

    [[nodiscard]] processor_details const& details() const noexcept {
        return details_;
    }
private:
    relation::graph_type* relations_{};
    yugawara::compiled_info const* info_{};
    impl::scopes_info scopes_info_{};
    impl::scope_indices scope_indices_{};
    processor_details details_{};

    processor_details create_details() {
        relation::graph_type g;
        bool has_scan_operator = false;
        bool has_emit_operator = false;
        bool has_write_operator = false;
        using kind = relation::expression_kind;
        takatori::relation::sort_from_upstream(*relations_, [&](relation::expression& node) {
            switch(node.kind()) {
                case kind::scan:
                    has_scan_operator = true;
                    break;
                case kind::emit:
                    has_emit_operator = true;
                    break;
                case kind::write:
                    has_write_operator = true;
                    break;
                default:
                    break;
            }
        });
        return {has_scan_operator, has_emit_operator, has_write_operator};
    }
};

}


