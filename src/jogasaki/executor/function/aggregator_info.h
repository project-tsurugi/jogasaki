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

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include "functions.h"

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using takatori::util::enum_tag_t;

class aggregator_info {
public:
    aggregator_info() = default;
    ~aggregator_info() = default;
    aggregator_info(aggregator_info const& other) = default;
    aggregator_info& operator=(aggregator_info const& other) = default;
    aggregator_info(aggregator_info&& other) noexcept = default;
    aggregator_info& operator=(aggregator_info&& other) noexcept = default;

    explicit aggregator_info(
        aggregator_type aggregator
    ) :
        aggregator_(std::move(aggregator))
    {}

    [[nodiscard]] aggregator_type const& aggregator() const noexcept {
        return aggregator_;
    }
private:
    aggregator_type aggregator_{};
};

}
