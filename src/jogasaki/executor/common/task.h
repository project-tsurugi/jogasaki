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

#include <atomic>
#include <ostream>
#include <string_view>

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/utils/interference_size.h>

#include "step.h"

namespace jogasaki::executor::common {

class task : public model::task {
public:
    using step_type = class step;

    task();

    task(
        request_context* context,
        step* src
    );

    [[nodiscard]] identity_type id() const override;

    [[nodiscard]] step_type* step() const;

    [[nodiscard]] request_context* context() const;

    [[nodiscard]] bool has_transactional_io() override;

    [[nodiscard]] model::task_transaction_kind transaction_capability() override;
protected:
    std::ostream& write_to(std::ostream& out) const override;;

private:
    cache_align static inline std::atomic_size_t id_src = 10000; //NOLINT

    identity_type id_{};
    request_context* context_{};
    step_type* src_{};
};

}



