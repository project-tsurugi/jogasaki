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

#include <memory>
#include <glog/logging.h>

#include <takatori/relation/step/join.h>
#include <takatori/util/downcast.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/utils/iterator_pair.h>
#include <jogasaki/utils/iterator_incrementer.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include "join_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

class join : public cogroup_operator {
public:
    friend class join_context;

    using input_index = std::size_t;

    using join_kind = takatori::relation::join_kind;

    using iterator = data::iterable_record_store::iterator;
    using iterator_pair = utils::iterator_pair<iterator>;

    join() = default;

    join(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        join_kind kind,
        takatori::util::optional_ptr<takatori::scalar::expression const> expression,
        std::unique_ptr<operator_base> downstream = nullptr
    ) : cogroup_operator(index, info, block_index),
        kind_(kind),
        evaluator_(create_evaluator(expression, info.compiled_info())),
        has_condition_(expression.has_value()),
        downstream_(std::move(downstream))
    {}

    /**
     * @brief create context (if needed) and process cogroup
     * @param context task-wide context used to create operator context
     * @param cgrp the cogroup to process
     */
    void process_cogroup(abstract::task_context* context, cogroup& cgrp) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<join_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<join_context>(
                index(),
                ctx.block_scope(block_index()),
                ctx.resource(),
                ctx.varlen_resource()
            );
        }
        (*this)(*p, cgrp, context);
    }

    /**
     * @brief process record with context object
     * @param ctx operator context object for the execution
     */
    void operator()(join_context& ctx, cogroup& cgrp, abstract::task_context* context = nullptr) {
        std::vector<iterator_pair> iterators{};
        iterators.reserve(cgrp.groups().size());
        for(auto&& g : cgrp.groups()) {
            iterators.emplace_back(g.begin(), g.end());
        }
        auto target = ctx.variables().store().ref();
        bool cont = true;
        std::size_t n = iterators.size();
        utils::iterator_incrementer<iterator> incr{std::move(iterators)};
        while(cont) {
            auto& cur = incr.current();
            for(std::size_t i=0; i < n; ++i) { // TODO assign only first one when semi/anti-join
                auto&& g = cgrp.groups()[i];
                if (g.empty()) continue; // TODO outer join
                auto it = cur[i].first;
                for(auto&& f : g.fields()) {
                    auto src = f.is_key_ ? g.key() : accessor::record_ref{*it, g.record_size()};
                    utils::copy_field(f.type_, target, f.target_offset_, src, f.source_offset_, ctx.varlen_resource()); // TODO no need to copy between resources
                }
            }
            auto resource = ctx.varlen_resource();
            auto& scope = ctx.variables();
            bool res = true;
            if (has_condition_) {
                utils::checkpoint_holder cp{resource};
                res = evaluator_(scope, resource).to<bool>();
            }
            if (res && downstream_) {
                unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
            }
            if(! incr.increment()) {
                break;
            }
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::join;
    }

private:
    join_kind kind_{};
    expression::evaluator evaluator_{};
    bool has_condition_{};
    std::unique_ptr<operator_base> downstream_{};

    expression::evaluator create_evaluator(
        takatori::util::optional_ptr<takatori::scalar::expression const> expression,
        yugawara::compiled_info const& compiled_info
    ) {
        if (expression) {
            return expression::evaluator(*expression, compiled_info);
        }
        return {};
    }
};

}