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

#include <vector>
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/executor/function/aggregate_function_kind.h>
#include <jogasaki/executor/function/aggregator_info.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using takatori::util::enum_tag_t;

class aggregate_function_info_base {
public:
    constexpr aggregate_function_kind kind() const noexcept {
        return kind_;
    }
    aggregate_function_info_base(aggregate_function_kind kind) : kind_(kind) {}
    virtual ~aggregate_function_info_base() = default;

    [[nodiscard]] aggregator_info const& pre() const noexcept { return pre_; };
    [[nodiscard]] aggregator_info const& mid() const noexcept { return mid_; };
    [[nodiscard]] aggregator_info const& post() const noexcept { return post_; };

    void pre(aggregator_info&& arg) noexcept { pre_ = std::move(arg); };
    void mid(aggregator_info&& arg) noexcept { mid_ = std::move(arg); };
    void post(aggregator_info&& arg) noexcept { post_ = std::move(arg); };

    virtual void register_aggregators() noexcept = 0;
protected:
    aggregate_function_kind kind_;

private:
    aggregator_info pre_{};
    aggregator_info mid_{};
    aggregator_info post_{};
};

template <aggregate_function_kind Kind>
class aggregate_function_info;

template <>
class aggregate_function_info<aggregate_function_kind::sum> : public aggregate_function_info_base {
public:
    explicit aggregate_function_info(enum_tag_t<aggregate_function_kind::sum>) : aggregate_function_info_base(aggregate_function_kind::sum) {}
    void register_aggregators() noexcept override;
private:
};

template <aggregate_function_kind Kind>
aggregate_function_info(enum_tag_t<Kind>) -> aggregate_function_info<Kind>;


}
