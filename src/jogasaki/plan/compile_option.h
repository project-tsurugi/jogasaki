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

#include <cstddef>
#include <functional>

#include <yugawara/compiler_result.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/variable/configurable_provider.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/aggregate/configurable_provider.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::plan {

/**
 * @brief compile option to customize compile behavior
 */
class compile_option {
public:

    void explain_by_text_only(bool arg) noexcept {
        explain_by_text_only_ = arg;
    }

    [[nodiscard]] bool explain_by_text_only() const noexcept {
        return explain_by_text_only_;
    }

private:
    ///@brief indicates the compile request is only to explain by text
    bool explain_by_text_only_{false};

};

}
