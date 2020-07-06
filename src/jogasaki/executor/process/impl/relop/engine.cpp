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
#include "engine.h"

#include <jogasaki/storage/storage_context.h>

#include "scanner.h"

namespace jogasaki::executor::process::impl::relop {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

engine::engine(
    graph::graph<relation::expression>& relations,
    std::shared_ptr<compiled_info> compiled_info,
    std::shared_ptr<impl::processor_variables> variables
) noexcept :
    relations_(relations),
    compiled_info_(std::move(compiled_info)),
    variables_(std::move(variables))
{}

relation::expression &engine::head() {
    relation::expression* result = nullptr;
    takatori::relation::enumerate_top(relations_, [&](relation::expression& v) {
        result = &v;
    });
    if (result != nullptr) {
        return *result;
    }
    fail();
}

void engine::operator()(const relation::find &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::scan &node) {
    LOG(INFO) << "scan";
    if (operators_.count(std::addressof(node)) == 0) {
        auto stg = std::make_shared<storage::storage_context>();
        std::map<std::string, std::string> options{};
        stg->open(options);
        operators_[std::addressof(node)] = std::make_unique<scanner>();
    }
    auto&s = *static_cast<scanner*>(operators_[std::addressof(node)].get());
    dispatch(*this, node.output().opposite()->owner());
}

void engine::operator()(const relation::join_find &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::join_scan &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::project &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::filter &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::buffer &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::emit &node) {
    (void)node;
    LOG(INFO) << "emit";
    if (!emitter_) {
        emitter_ = std::make_shared<emitter>(meta_, store_);
    }
    emitter_->emit(buf_.ref());
}

void engine::operator()(const relation::write &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::values &node) {
    (void)node;
    fail();
}
void engine::operator()(const relation::step::join &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::aggregate &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::intersection &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::difference &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::flatten &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::take_flat &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::take_group &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::take_cogroup &node) {
    (void)node;
    fail();
}

void engine::operator()(const relation::step::offer &node) {
    (void)node;
    fail();
}

void engine::process() {
    dispatch(*this, head());
}
}


