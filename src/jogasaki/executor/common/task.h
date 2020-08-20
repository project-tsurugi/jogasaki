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

#include <atomic>
#include <ostream>
#include <string_view>

#include <jogasaki/model/task.h>
#include <jogasaki/utils/interference_size.h>
#include "step.h"

namespace jogasaki::executor::common {

class task : public model::task {
public:
    using step_type = class step;

    task() {
        id_ = id_src++;
    }

    task(request_context* context,
            step* src) : context_(context), src_(src) {
        id_ = id_src++;
    }

    [[nodiscard]] identity_type id() const override {
        return id_;
    }

    [[nodiscard]] step_type* step() const {
        return src_;
    }

    [[nodiscard]] request_context* context() const {
        return context_;
    }

protected:
    std::ostream& write_to(std::ostream& out) const override {
        using namespace std::string_view_literals;
        return out << "task[id="sv << std::to_string(static_cast<identity_type>(id_)) << "]"sv;
    };

private:
    cache_align static inline std::atomic_size_t id_src = 10000;
    identity_type id_{};
    request_context* context_{};
    step_type* src_{};
};

}



