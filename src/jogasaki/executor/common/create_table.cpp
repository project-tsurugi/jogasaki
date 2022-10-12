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

#include <yugawara/binding/extract.h>
#include <takatori/util/fail.h>

#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/constants.h>

#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::executor::common {

using takatori::util::fail;

std::string create_metadata(std::string_view sql_text) {
    proto::metadata::storage::Table t{};
    proto::metadata::storage::TexualDefinition text{};
    t.set_format_version(metadata_format_version);
    t.set_allocated_statement(&text);
    text.set_ddl_statement(std::string{sql_text});

    std::stringstream ss{};
    if (!t.SerializeToOstream(&ss)) {
        fail();
    }
    t.release_statement();
    return ss.str();
}

create_table::create_table(
    takatori::statement::create_table& ct,
    std::string_view sql_text
) noexcept:
    ct_(std::addressof(ct)),
    metadata_(create_metadata(sql_text))
{}

model::statement_kind create_table::kind() const noexcept {
    return model::statement_kind::create_table;
}

bool create_table::operator()(request_context& context) const {
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    auto& provider = *context.storage_provider();
    auto c = yugawara::binding::extract_shared<yugawara::storage::table>(ct_->definition());
    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->primary_key());
    auto rh = std::any_cast<plan::storage_processor_result>(ct_->runtime_hint());
    if(rh.primary_key_generated()) {
        auto p = rh.primary_key_sequence();
        BOOST_ASSERT(p->definition_id()); //NOLINT
        context.sequence_manager()->register_sequence(
            std::addressof(static_cast<kvs::transaction&>(*context.transaction())),
            *p->definition_id(),
            p->simple_name(),
            p->initial_value(),
            p->increment_value(),
            p->min_value(),
            p->max_value(),
            p->cycle(),
            true
        );
        provider.add_sequence(p);
    }
    std::shared_ptr<yugawara::storage::table const> t{};
    try {
        t = provider.add_table(c, false);
    } catch(std::invalid_argument& e) {
        VLOG(log_error) << "table " << c->simple_name() << " already exists";
        context.status_code(status::err_already_exists);
        return false;
    }
    BOOST_ASSERT(i.ownership());  //NOLINT
    try {
        provider.add_index(i.ownership(), false);
    } catch(std::invalid_argument& e) {
        VLOG(log_error) << "primary index " << i->simple_name() << " already exists";
        context.status_code(status::err_already_exists);
        return false;
    }
    sharksfin::StorageOptions options{};
    options.payload(metadata_);
    if(auto stg = context.database()->create_storage(c->simple_name(), options);! stg) {
        VLOG(log_error) << "storage " << c->simple_name() << " already exists ";
    }
    return true;
}

}
