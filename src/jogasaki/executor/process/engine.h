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
#include <takatori/util/string_builder.h>
#include <takatori/util/fail.h>

#include <jogasaki/utils/aligned_unique_ptr.h>
#include <jogasaki/meta/record_meta.h>

#include "processor.h"
#include "scanner.h"
#include "emitter.h"

namespace jogasaki::executor::process {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

class engine {
public:
    using compiler_result = yugawara::compiler_result;

    engine() = default;
    ~engine() = default;
    engine(engine const& other) = default;
    engine& operator=(engine const& other) = default;
    engine(engine&& other) noexcept = default;
    engine& operator=(engine&& other) noexcept = default;

    explicit engine(graph::graph<relation::expression>& operators,
            std::shared_ptr<meta::record_meta> meta,
            std::shared_ptr<data::record_store> store
            ) noexcept :
            operators_(operators),
            meta_(std::move(meta)),
            store_(std::move(store)),
            buf_(utils::make_aligned_array<char>(meta_->record_alignment(), meta_->record_size())) {
        //TODO prepare stack-like working area needed for this engine to complete all operators
    }

    relation::expression& head() {
        relation::expression* result = nullptr;
        takatori::relation::enumerate_top(operators_, [&](relation::expression& v) {
            result = &v;
        });
        if (result != nullptr) {
            return *result;
        }
        fail();
    }

    void operator()(relation::find const& node) {
        takatori::util::fail();
    }
    void operator()(relation::scan const& node) {
        LOG(INFO) << "scan";
        auto stg = std::make_shared<storage::storage_context>();
        std::map<std::string, std::string> options{};
        stg->open(options);
        scanner s{{}, stg, meta_, accessor::record_ref{buf_.get(), meta_->record_size()}};
        dispatch(*this, node.output().opposite()->owner());
    }
    void operator()(relation::join_find const& node) {
        fail();
    }
    void operator()(relation::join_scan const& node) {
        fail();
    }
    void operator()(relation::project const& node) {
        fail();
    }
    void operator()(relation::filter const& node) {
        fail();
    }
    void operator()(relation::buffer const& node) {
        fail();
    }
    void operator()(relation::emit const& node) {
        LOG(INFO) << "emit";
        if (!emitter_) {
            emitter_ = std::make_shared<emitter>(meta_, store_);
        }
        emitter_->emit(accessor::record_ref{buf_.get(), meta_->record_size()});
    }
    void operator()(relation::write const& node) {
        fail();
    }

    void operator()(relation::step::join const& node) {
        fail();
    }
    void operator()(relation::step::aggregate const& node) {
        fail();
    }
    void operator()(relation::step::intersection const& node) {
        fail();
    }
    void operator()(relation::step::difference const& node) {
        fail();
    }
    void operator()(relation::step::flatten const& node) {
        fail();
    }
    void operator()(relation::step::take_flat const& node) {
        fail();
    }
    void operator()(relation::step::take_group const& node) {
        fail();
    }
    void operator()(relation::step::take_cogroup const& node) {
        fail();
    }
    void operator()(relation::step::offer const& node) {
        fail();
    }

    void process() {
        dispatch(*this, head());
    }

private:
    graph::graph<relation::expression>& operators_;
    std::shared_ptr<compiler_result> compiled_{};
    std::shared_ptr<meta::record_meta> meta_{};
    utils::aligned_array<char> buf_;
    std::shared_ptr<data::record_store> store_{};
    std::shared_ptr<emitter> emitter_{};
};

}


