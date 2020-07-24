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
#include <jogasaki/executor/process/impl/block_variables_info.h>

namespace jogasaki::executor::process {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

/**
 * @brief processor specification
 */
class processor_info {
public:
    processor_info() = default;

    processor_info(
        graph::graph<relation::expression>& relations,
        yugawara::compiled_info const& info
    ) :
        relations_(std::addressof(relations)),
        info_(std::addressof(info))
    {
        auto&& p = impl::create_block_variables(*relations_, *info_);
        blocks_info_ = std::move(p.first);
        block_indices_ = std::move(p.second);
    }

    [[nodiscard]] graph::graph<relation::expression>& relations() const noexcept {
        return *relations_;
    }

    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept {
        return *info_;
    }

    [[nodiscard]] impl::blocks_info_type const& blocks_info() const noexcept {
        return blocks_info_;
    }

    [[nodiscard]] impl::block_indices_type const& block_indices() const noexcept {
        return block_indices_;
    }
private:
    graph::graph<relation::expression>* relations_{};
    yugawara::compiled_info const* info_{};
    impl::blocks_info_type blocks_info_{};
    impl::block_indices_type block_indices_{};
};

}


