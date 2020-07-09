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

namespace jogasaki::executor::process::impl {

/**
 * @brief block variables meta data shared by multiple threads
 */
class block_variables_info {
public:
    block_variables_info() = default;

    explicit block_variables_info(
        std::unique_ptr<variable_value_map> value_map,
        std::shared_ptr<meta::record_meta> meta
    ) : value_map_(std::move(value_map)), meta_(std::move(meta))
    {}

    [[nodiscard]] variable_value_map& value_map() const noexcept {
        return *value_map_;
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

private:
    std::unique_ptr<variable_value_map> value_map_{};
    std::shared_ptr<meta::record_meta> meta_{};
};

}


