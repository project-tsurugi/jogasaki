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

#include "processor.h"
#include "engine.h"

namespace jogasaki::executor::process {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

class processor_impl : public processor {
public:
    using compiler_result = yugawara::compiler_result;

    processor_impl() = default;
    ~processor_impl() override = default;
    processor_impl(processor_impl const& other) = default;
    processor_impl& operator=(processor_impl const& other) = default;
    processor_impl(processor_impl&& other) noexcept = default;
    processor_impl& operator=(processor_impl&& other) noexcept = default;

    explicit processor_impl(graph::graph<relation::expression>& operators) noexcept : operators_(operators) {}

    status run(task_context* context) override {
        engine visitor{operators_, {}};
        return status::completed;
    }

private:
    graph::graph<relation::expression>& operators_;
};

}


