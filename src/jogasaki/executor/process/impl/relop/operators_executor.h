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

#include <glog/logging.h>

#include <yugawara/compiler_result.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/dispatch.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/impl/relop/relational_operators.h>

#include "emit.h"

namespace jogasaki::executor::process::impl::relop {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;
using yugawara::compiled_info;

class operators_executor {
public:
    operators_executor() = delete;
    ~operators_executor() = default;
    operators_executor(operators_executor const& other) = delete;
    operators_executor& operator=(operators_executor const& other) = delete;
    operators_executor(operators_executor&& other) noexcept = delete;
    operators_executor& operator=(operators_executor&& other) noexcept = delete;

    operators_executor(
        graph::graph<relation::expression>& relations,
        std::shared_ptr<compiled_info> compiled_info,
        std::shared_ptr<relational_operators> operators
    ) noexcept;

    relation::expression& head();

    template <class T>
    T& to(const relation::expression &node) {
        return *static_cast<T*>((operators_->operators()).at(std::addressof(node)).get());
    }

    void operator()(relation::find const& node);
    void operator()(relation::scan const& node);
    void operator()(relation::join_find const& node);
    void operator()(relation::join_scan const& node);
    void operator()(relation::project const& node);
    void operator()(relation::filter const& node);
    void operator()(relation::buffer const& node);
    void operator()(relation::emit const& node);
    void operator()(relation::write const& node);
    void operator()(relation::values const& node);
    void operator()(relation::step::join const& node);
    void operator()(relation::step::aggregate const& node);
    void operator()(relation::step::intersection const& node);
    void operator()(relation::step::difference const& node);
    void operator()(relation::step::flatten const& node);
    void operator()(relation::step::take_flat const& node);
    void operator()(relation::step::take_group const& node);
    void operator()(relation::step::take_cogroup const& node);
    void operator()(relation::step::offer const& node);

    void process();

private:
    graph::graph<relation::expression>& relations_;
    std::shared_ptr<compiled_info> compiled_info_{};
    std::shared_ptr<relational_operators> operators_{};
};

}


