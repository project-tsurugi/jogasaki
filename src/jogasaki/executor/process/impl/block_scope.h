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
#include <jogasaki/meta/record_meta.h>
#include "variable_value_map.h"
#include "block_scope_info.h"

namespace jogasaki::executor::process::impl {

/**
 * @brief block scoped variables storage
 */
class block_scope {
public:
    /**
     * @brief construct empty instance
     */
    block_scope() = default;

    /**
     * @brief construct new instance
     */
    explicit block_scope(
        block_scope_info const& info
    );

    /**
     * @brief accessor to variable store
     */
    [[nodiscard]] data::small_record_store& store() const noexcept;

    /**
     * @brief accessor to variable value map
     */
    [[nodiscard]] variable_value_map const& value_map() const noexcept;

    /**
     * @brief accessor to metadata of variable store
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

private:
    block_scope_info const* info_{};
    std::unique_ptr<data::small_record_store> store_{};
};

}


