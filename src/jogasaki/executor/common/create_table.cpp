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
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/storage_options.h>

namespace jogasaki::executor::common {

using takatori::util::fail;

static std::string create_metadata(std::string_view sql_text) {
    proto::metadata::storage::Storage st{};
    proto::metadata::storage::TexualDefinition text{};
    st.set_message_version(metadata_format_version);
    st.set_allocated_statement(&text);
    text.set_ddl_statement(std::string{sql_text});

    std::stringstream ss{};
    if (!st.SerializeToOstream(&ss)) {
        fail();
    }
    st.release_statement();
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
    if(provider.find_table(c->simple_name())) {
        VLOG(log_info) << "Table " << c->simple_name() << " already exists.";
        context.status_code(status::err_already_exists);
        return false;
    }
    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->primary_key());
    auto rh = std::any_cast<plan::storage_processor_result>(ct_->runtime_hint());
    if(rh.primary_key_generated()) {
        auto p = rh.primary_key_sequence();
        executor::sequence::metadata_store ms{*context.transaction()->object()};
        std::size_t def_id{};
        if(! ms.find_next_empty_def_id(def_id)) {
            VLOG(log_error) << "assigning sequence def id failed";
            context.status_code(status::err_unknown);
            return false;
        }
        p->definition_id(def_id); // TODO p is part of prepared statement, avoid updating it directly
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
//        provider.add_sequence(p);
    }

    std::string storage{};
    yugawara::storage::configurable_provider target{};
    if(auto res = recovery::create_storage_option(*i, storage, utils::metadata_serializer_option{false}); ! res) {
        context.status_code(status::err_already_exists);
        return res;
    }
    if(auto res = recovery::deserialize_storage_option_into_provider(storage, provider, provider, true); ! res) {
        context.status_code(status::err_unknown);
        return false;
    }

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage(c->simple_name(), options);! stg) {
        VLOG(log_info) << "storage " << c->simple_name() << " already exists ";
        // recovery possibly issues CREATE TABLE ddl and in that case storage already exists.
        // TODO when recovery stops using CREATE TABLE, handle this as error
    }
    return true;
}

}
