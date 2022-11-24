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
#include "task_factory.h"

namespace jogasaki::scheduler {

details::custom_task::custom_task() = default;

details::custom_task::custom_task(task_body_type body, bool transactional_io) :
    body_(std::move(body)),
    transactional_io_(transactional_io)
{}

model::task::identity_type details::custom_task::id() const {
    return id_;
}

model::task_result details::custom_task::operator()() {
    return body_();
}

bool details::custom_task::has_transactional_io() {
    return transactional_io_;
}

std::ostream& details::custom_task::write_to(std::ostream& out) const {
    using namespace std::string_view_literals;
    return out << "custom_task[id="sv << std::to_string(static_cast<identity_type>(id_)) << "]"sv;
}

flat_task create_custom_task(request_context* rctx, task_body_type body, bool has_transaction_io, bool delayed) {
    return flat_task{
        task_enum_tag<flat_task_kind::wrapped>,
        rctx,
        std::make_shared<details::custom_task>(
            std::move(body),
            has_transaction_io
        ),
        delayed
    };
}
} // namespace jogasaki::scheduler
