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

#include <unordered_set>
#include <optional>

#include <takatori/util/optional_ptr.h>
#include <takatori/util/downcast.h>

#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>

namespace jogasaki::executor::common {

using takatori::util::unsafe_downcast;

/**
 * @brief graph common implementation
 */
class write : public model::statement {
public:
    write() = default;

    explicit write(request_context& context) noexcept :
        context_(std::addressof(context))
    {}

    void context(request_context& context) {
        context_ = std::addressof(context);
    }

    [[nodiscard]] request_context* context() const noexcept {
        return context_;
    }

    [[nodiscard]] model::statement_kind kind() const noexcept override {
        return model::statement_kind::write;
    }

private:
    request_context* context_{};
};

}
