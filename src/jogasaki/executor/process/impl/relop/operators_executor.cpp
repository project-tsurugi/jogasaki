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
#include "operators_executor.h"

#include <jogasaki/storage/storage_context.h>
#include <jogasaki/executor/process/impl/work_context.h>

#include "scan.h"
#include "emit.h"

namespace jogasaki::executor::process::impl::relop {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

operators_executor::operators_executor(
    graph::graph<relation::expression>& relations,
    compiled_info const* compiled_info,
    relational_operators* operators,
    abstract::task_context *context
) noexcept :
    relations_(relations),
    compiled_info_(compiled_info),
    operators_(operators),
    context_(context)
{}

relation::expression &operators_executor::head() {
    relation::expression* result = nullptr;
    takatori::relation::enumerate_top(relations_, [&](relation::expression& v) {
        result = &v;
    });
    if (result != nullptr) {
        return *result;
    }
    fail();
}

void operators_executor::operator()(const relation::find &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::scan &node) {
    LOG(INFO) << "scan";
    auto&s = to<scan>(node);
    auto b = s.block_index();
    (void)b;
    dispatch(*this, node.output().opposite()->owner());
}

void operators_executor::operator()(const relation::join_find &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::join_scan &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::project &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::filter &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::buffer &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::emit &node) {
    auto&s = to<emit>(node);
    auto* ctx = find_context<emit_context>(&s);
    if (! ctx) {
        ctx = make_context<emit_context>(&s);  // TODO fill args
    }
    s(*ctx);
}

void operators_executor::operator()(const relation::write &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::values &node) {
    (void)node;
    fail();
}
void operators_executor::operator()(const relation::step::join &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::aggregate &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::intersection &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::difference &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::flatten &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::take_flat &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::take_group &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::take_cogroup &node) {
    (void)node;
    fail();
}

void operators_executor::operator()(const relation::step::offer &node) {
    (void)node;
    fail();
}

void operators_executor::process() {
    dispatch(*this, head());
}
}


