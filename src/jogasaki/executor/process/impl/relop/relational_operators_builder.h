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
#include <takatori/util/fail.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/offer.h>
#include <yugawara/compiled_info.h>

#include <yugawara/compiler_result.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/dispatch.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/relop/operator_base.h>
#include <jogasaki/storage/storage_context.h>
#include "relational_operators.h"
#include "scan.h"
#include "emit.h"

namespace jogasaki::executor::process::impl::relop {

namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

/**
 * @brief generator for relational operators
 */
class relational_operators_builder {
public:
    using operators_type = relational_operators::operators_type;

    relational_operators_builder() = default;

    explicit relational_operators_builder(
        std::shared_ptr<processor_info> info,
        memory::paged_memory_resource* resource = nullptr) :
        info_(std::move(info))
    {
        (void)resource;
    }

    relational_operators operator()() && {
        dispatch(*this, head());
        return relational_operators{std::move(operators_)};
    }

    relation::expression& head() {
        relation::expression* result = nullptr;
        takatori::relation::enumerate_top(info_->operators(), [&](relation::expression& v) {
            result = &v;
        });
        if (result != nullptr) {
            return *result;
        }
        fail();
    }

    void operator()(relation::find const& node) {
        (void)node;
    }
    void operator()(relation::scan const& node) {
        LOG(INFO) << "scan op created";
        if (operators_.count(std::addressof(node)) == 0) {
            auto stg = std::make_shared<storage::storage_context>();
            std::map<std::string, std::string> options{};
            stg->open(options);
            operators_[std::addressof(node)] = std::make_unique<scan>();
        }
        dispatch(*this, node.output().opposite()->owner());
    }
    void operator()(relation::join_find const& node) {
        (void)node;
    }
    void operator()(relation::join_scan const& node) {
        (void)node;
    }
    void operator()(relation::project const& node) {
        (void)node;
    }
    void operator()(relation::filter const& node) {
        (void)node;
    }
    void operator()(relation::buffer const& node) {
        (void)node;
    }
    void operator()(relation::emit const& node) {
        LOG(INFO) << "emit op created";
        if (operators_.count(std::addressof(node)) == 0) {
            operators_[std::addressof(node)] = std::make_unique<emit>();
        }
    }

    void operator()(relation::write const& node) {
        (void)node;
    }
    void operator()(relation::values const& node) {
        (void)node;
    }
    void operator()(relation::step::join const& node) {
        (void)node;
    }
    void operator()(relation::step::aggregate const& node) {
        (void)node;
    }
    void operator()(relation::step::intersection const& node) {
        (void)node;
    }
    void operator()(relation::step::difference const& node) {
        (void)node;
    }
    void operator()(relation::step::flatten const& node) {
        (void)node;
    }
    void operator()(relation::step::take_flat const& node) {
        (void)node;
    }
    void operator()(relation::step::take_group const& node) {
        (void)node;
    }
    void operator()(relation::step::take_cogroup const& node) {
        (void)node;
    }
    void operator()(relation::step::offer const& node) {
        (void)node;
    }

private:
    std::shared_ptr<processor_info> info_{};
    operators_type operators_{};
};

inline relational_operators create_relational_operators(
    std::shared_ptr<processor_info> info,
    memory::paged_memory_resource* resource = nullptr
) {
    return relational_operators_builder{info, resource}();
}

}

