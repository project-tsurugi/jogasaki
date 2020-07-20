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

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/group_reader.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::relop {

/**
 * @brief take_group context
 */
class take_group_context : public context_base {
public:
    friend class take_group;
    /**
     * @brief create empty object
     */
    take_group_context() = default;

    /**
     * @brief create new object
     */
    explicit take_group_context(
        std::shared_ptr<meta::record_meta> meta,
        block_variables_info const& info
    ) : context_base(std::make_shared<block_variables>(info)),
    store_(std::move(meta))
    {}

    operator_kind kind() override {
        return operator_kind::take_group;
    }
private:
    data::small_record_store store_{};
    group_reader* reader_{};
};

}


