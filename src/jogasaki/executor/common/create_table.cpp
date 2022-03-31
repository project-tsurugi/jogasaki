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
#include "create_table.h"

#include <takatori/statement/create_table.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/binding/factory.h>
#include <yugawara/binding/extract.h>

#include <jogasaki/logging.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>

namespace jogasaki::executor::common {


create_table::create_table(takatori::statement::create_table& ct) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind create_table::kind() const noexcept {
    return model::statement_kind::create_table;
}

bool create_table::operator()(request_context& context) const {
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    auto& provider = *context.storage_provider();
    auto c = yugawara::binding::extract_shared<yugawara::storage::table>(ct_->definition());
    auto t = provider.add_table(c, false);

    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->primary_key());
    BOOST_ASSERT(i.ownership());  //NOLINT
    provider.add_index(i.ownership(), false);
    if(auto stg = context.database()->get_or_create_storage(c->simple_name());! stg) {
        VLOG(log_error) << "storage " << c->simple_name() << " already exists ";
    }
    return true;
}
}
