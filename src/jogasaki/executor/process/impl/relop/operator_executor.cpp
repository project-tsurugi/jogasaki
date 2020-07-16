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

#include <jogasaki/storage/storage_context.h>

#include "scan.h"
#include "scan_context.h"
#include "emit.h"
#include "emit_context.h"
#include "take_group.h"
#include "offer.h"

namespace jogasaki::executor::process::impl::relop {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

operator_executor::operator_executor(
    graph::graph<relation::expression>& relations,
    compiled_info const* compiled_info,
    operator_container* operators,
    abstract::task_context *context
) noexcept :
    relations_(relations),
    compiled_info_(compiled_info),
    operators_(operators),
    context_(context)
{}

relation::expression &operator_executor::head() {
    relation::expression* result = nullptr;
    takatori::relation::enumerate_top(relations_, [&](relation::expression& v) {
        result = &v;
    });
    if (result != nullptr) {
        return *result;
    }
    fail();
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
        auto stg = std::make_shared<storage::storage_context>();
        ctx = make_context<scan_context>(&s, std::move(stg));
    }
    s(*ctx);
    dispatch(*this, node.output().opposite()->owner());
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
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::filter &node) {
    (void)node;
    fail();
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
        ctx = make_context<emit_context>(&s, s.meta());
    }
    s(*ctx);
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
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::take_group &node) {
    auto&s = to<take_group>(node);
    auto* ctx = find_context<take_group_context>(&s);
    if (! ctx) {
        ctx = make_context<take_group_context>(&s);
    }
    s(*ctx);
    dispatch(*this, node.output().opposite()->owner());
}

void operator_executor::operator()(const relation::step::take_cogroup &node) {
    (void)node;
    fail();
}

void operator_executor::operator()(const relation::step::offer &node) {
    auto&s = to<offer>(node);
    auto* ctx = find_context<offer_context>(&s);
    if (! ctx) {
        ctx = make_context<offer_context>(&s, s.meta());
    }
    s(*ctx);
}

void operator_executor::process() {
    dispatch(*this, head());
}

}


