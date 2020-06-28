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

#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/impl/relop/engine.h>

namespace jogasaki::executor::process::impl {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

/**
 * @brief processor implementation for production
 */
class processor : public process::abstract::processor {
public:
    using compiler_result = yugawara::compiler_result;

    processor() = default;
    ~processor() override = default;
    processor(processor const& other) = default;
    processor& operator=(processor const& other) = default;
    processor(processor&& other) noexcept = default;
    processor& operator=(processor&& other) noexcept = default;

    explicit processor(graph::graph<relation::expression>& operators) noexcept : operators_(std::addressof(operators)) {}

    abstract::status run(abstract::task_context* context) override {
        (void) context;
        impl::relop::engine visitor{*operators_, {}, {}};
        return abstract::status::completed;
    }

private:
    graph::graph<relation::expression>* operators_;
};

}


