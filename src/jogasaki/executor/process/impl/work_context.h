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

#include <jogasaki/executor/process/abstract/work_context.h>
#include "processor_variables.h"

namespace jogasaki::executor::process::impl {

/**
 * @brief processor working context implementation for production
 */
class work_context : public process::abstract::work_context {
public:
    work_context() = default;

    work_context(
        std::shared_ptr<processor_info> info,
        memory::paged_memory_resource* resource = nullptr) :
        variables_(std::move(info), resource)
    {}

    [[nodiscard]] processor_variables const& variables() const noexcept {
        return variables_;
    }
private:
    processor_variables variables_{};
};

}


