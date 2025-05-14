/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <vector>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/transaction_context.h>

#include "context_base.h"
#include "index_matcher.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

template<class MatchInfo>
class matcher;

}  // namespace details

template<class MatchInfo>
class index_join;

/**
 * @brief context object for index_join
 */
template <class MatchInfo>
class index_join_context : public context_base {
public:

    friend class index_join<MatchInfo>;

    /**
     * @brief create empty object
     */
    index_join_context() = default;

    /**
     * @brief create new object
     */
    index_join_context(
        class abstract::task_context* ctx,
        variable_table& input_variables,
        variable_table& output_variables,
        std::unique_ptr<kvs::storage> primary_stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        transaction_context* tx,
        std::unique_ptr<details::matcher<MatchInfo>> matcher,
        memory_resource* resource,
        memory_resource* varlen_resource,
        kvs::transaction* strand
    ) :
        context_base(ctx, input_variables, output_variables, resource, varlen_resource),
        primary_stg_(std::move(primary_stg)),
        secondary_stg_(std::move(secondary_stg)),
        tx_(tx),
        matcher_(std::move(matcher)),
        strand_(strand)
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return std::is_same_v<MatchInfo, details::match_info_find> ? operator_kind::join_find
                                                                   : operator_kind::join_scan;
    }

    void release() override {
        //TODO
    }

    [[nodiscard]] transaction_context* transaction() const noexcept {
        return tx_;
    }

private:
    std::unique_ptr<kvs::storage> primary_stg_{};
    std::unique_ptr<kvs::storage> secondary_stg_{};
    transaction_context* tx_{};
    std::unique_ptr<details::matcher<MatchInfo>> matcher_{};
    kvs::transaction* strand_{};
};

}  // namespace jogasaki::executor::process::impl::ops
