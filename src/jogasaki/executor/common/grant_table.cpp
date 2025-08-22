/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "grant_table.h"

#include <memory>

#include <jogasaki/request_context.h>
#include "process_grant_revoke.h"

namespace jogasaki::executor::common {

grant_table::grant_table(
    takatori::statement::grant_table& gt
) noexcept:
    gt_(std::addressof(gt))
{}

model::statement_kind grant_table::kind() const noexcept {
    return model::statement_kind::grant_table;
}

bool grant_table::operator()(request_context& context) const {
    return process_grant_revoke(true, context, gt_->elements());
}

}
