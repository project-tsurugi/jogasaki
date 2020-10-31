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
#include "operator_executor.h"

#include <jogasaki/kvs/database.h>

#include "scan.h"
#include "scan_context.h"
#include "emit.h"
#include "emit_context.h"
#include "filter.h"
#include "filter_context.h"
#include "project.h"
#include "project_context.h"
#include "take_group.h"
#include "offer.h"
#include "take_flat.h"

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

operator_executor::operator_executor(
    relation::graph_type& relations,
    compiled_info const& compiled_info,
    operator_container* operators,
    abstract::task_context *context,
    memory_resource* resource
) noexcept :
    relations_(std::addressof(relations)),
    compiled_info_(std::addressof(compiled_info)),
    operators_(operators),
    context_(context),
    resource_(resource)
{}

relation::expression &operator_executor::head() {
    relation::expression* result = nullptr;
    takatori::relation::enumerate_top(*relations_, [&](relation::expression& v) {
        result = &v;
    });
    if (result != nullptr) {
        return *result;
    }
    fail();
}

block_scope& operator_executor::get_block_variables(std::size_t index) {
    return static_cast<work_context *>(context_->work_context())->variables(index); //NOLINT
}

void operator_executor::operator()(const relation::find &node) {
    (void)node;
    (void)compiled_info_;
    fail();
}

void operator_executor::operator()(const relation::scan &node) {
    DLOG(INFO) << "scan op executed";
    auto&s = to<scan>(node);
    auto* ctx = find_context<scan_context>(&s);
    if (! ctx) {
        auto db = kvs::database::open(); // TODO retrieve from request context
        std::string_view name("T0"); // TODO from node.source().entity()
        auto stg = db->create_storage(name); // TODO use get_storage
        auto& block_vars = static_cast<work_context *>(context_->work_context())->variables(s.block_index()); //NOLINT
        ctx = make_context<scan_context>(&s, std::move(stg), block_vars, resource_);
    }
    s(*ctx, this);
}

void operator_executor::operator()(const relation::join_find &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::join_scan &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::project &node) {
    auto&s = to<project>(node);
    auto* ctx = find_context<project_context>(&s);
    if (! ctx) {
        ctx = make_context<project_context>(&s, get_block_variables(s.block_index()), resource_);
    }
    s(*ctx, this);
}

void operator_executor::operator()(const relation::filter &node) {
    auto&s = to<filter>(node);
    auto* ctx = find_context<filter_context>(&s);
    if (! ctx) {
        ctx = make_context<filter_context>(&s, get_block_variables(s.block_index()), resource_);
    }
    s(*ctx, this);
}

void operator_executor::operator()(const relation::buffer &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::emit &node) {
    DLOG(INFO) << "emit op executed";
    auto&s = to<emit>(node);
    auto* ctx = find_context<emit_context>(&s);
    if (! ctx) {
        ctx = make_context<emit_context>(&s, s.meta(), get_block_variables(s.block_index()), resource_);
    }
//    s(*ctx);
}

void operator_executor::operator()(const relation::write &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::values &node) {
    (void)node;
    fail();
}
void operator_executor::operator()(const relation::step::join &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::aggregate &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::intersection &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::difference &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::flatten &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::take_flat &node) {
    auto&s = to<take_flat>(node);
    auto* ctx = find_context<take_flat_context>(&s);
    if (! ctx) {
        ctx = make_context<take_flat_context>(&s, get_block_variables(s.block_index()), resource_);
    }
    s(*ctx, this);
}

void operator_executor::operator()(const relation::step::take_group &node) {
    auto&s = to<take_group>(node);
    auto* ctx = find_context<take_group_context>(&s);
    if (! ctx) {
        ctx = make_context<take_group_context>(&s, get_block_variables(s.block_index()), resource_);
    }
    s(*ctx, this);
}

void operator_executor::operator()(const relation::step::take_cogroup &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::offer &node) {
    auto&s = to<offer>(node);
    auto* ctx = find_context<offer_context>(&s);
    if (! ctx) {
        ctx = make_context<offer_context>(&s, s.meta(), get_block_variables(s.block_index()), resource_);
    }
    s(*ctx);
}

void operator_executor::operator()() {
    dispatch(*this, head());
    // TODO handling status code
}

}


