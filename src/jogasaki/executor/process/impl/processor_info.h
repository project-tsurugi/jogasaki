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

namespace jogasaki::executor::process::impl {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

/**
 * @brief processor specification
 */
class processor_info {
public:
    processor_info() = default;

    explicit processor_info(
        graph::graph<relation::expression>& operators,
        yugawara::compiled_info const& info
    ) noexcept :
        operators_(std::addressof(operators)),
        info_(std::addressof(info))
    {}

    [[nodiscard]] graph::graph<relation::expression> const& operators() {
        return *operators_;
    }

    [[nodiscard]] yugawara::compiled_info const* compiled_info() {
        return info_;
    }

private:
    graph::graph<relation::expression>* operators_{};
    yugawara::compiled_info const* info_{};
};

}


