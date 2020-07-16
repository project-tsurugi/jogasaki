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
#include <jogasaki/executor/process/impl/relop/operator_container.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/abstract/task_context.h>

#include "emit.h"

namespace jogasaki::executor::process::impl::relop {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;
using yugawara::compiled_info;

class operator_executor {
public:
    operator_executor() = delete;
    ~operator_executor() = default;
    operator_executor(operator_executor const& other) = delete;
    operator_executor& operator=(operator_executor const& other) = delete;
    operator_executor(operator_executor&& other) noexcept = delete;
    operator_executor& operator=(operator_executor&& other) noexcept = delete;

    operator_executor(
        graph::graph<relation::expression>& relations,
        compiled_info const* compiled_info,
        operator_container* operators,
        abstract::task_context *context
    ) noexcept;

    relation::expression& head();

    template <class T>
    T& to(const relation::expression &node) {
        return *static_cast<T*>(operators_->at(std::addressof(node)));
    }

    template<class T>
    T* find_context(relop::operator_base const* p) {
        auto& contexts = static_cast<work_context*>(context_->work_context())->contexts();  //NOLINT
        if (contexts.count(p) == 0) {
            return nullptr;
        }
        return static_cast<T*>(contexts.at(p));
    }

    template<class T, class ... Args>
    T* make_context(relop::operator_base const* p, Args&&...args) {
        auto& contexts = static_cast<work_context*>(context_->work_context())->contexts();  //NOLINT
        auto [it, b] = contexts.emplace(p, std::make_unique<T>(std::forward<Args>(args)...));
        if (!b) {
            return nullptr;
        }
        return static_cast<T*>(it->second.get());
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
    compiled_info const* compiled_info_{};
    operator_container* operators_{};
    abstract::task_context *context_{};
};

}


