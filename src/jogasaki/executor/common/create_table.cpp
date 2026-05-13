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
#include "create_table.h"

#include <any>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/auth/action_kind.h>
#include <jogasaki/auth/action_set.h>
#include <jogasaki/auth/authorized_users_action_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/ddl_common.h>
#include <jogasaki/executor/conv/create_default_value.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/validate_index_key_type.h>
#include <jogasaki/utils/validate_table_definition.h>

namespace jogasaki::executor::common {

using takatori::util::string_builder;

create_table::create_table(
    takatori::statement::create_table& ct
) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind create_table::kind() const noexcept {
    return model::statement_kind::create_table;
}


bool create_table::operator()(request_context& context) const {
    assert_with_exception(context.storage_provider());
    auto& provider = *context.storage_provider();
    auto c = yugawara::binding::extract_shared<yugawara::storage::table>(ct_->definition());
    if(provider.find_table(c->simple_name())) {
        set_error_context(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Table \"" << c->simple_name() << "\" already exists." << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }

    // currently no schema is supported, so only admin can create table
    if (auto& s = context.req_info().request_source()) {
        if(s->session_info().user_type() != tateyama::api::server::user_type::administrator) {
            auto username = s->session_info().username();
            VLOG_LP(log_error) << "insufficient authorization user:\""
                               << (username.has_value() ? username.value() : "")
                               << "\"";
            set_error_context(
                context,
                error_code::permission_error,
                "insufficient authorization for the requested operation",
                status::err_illegal_operation
            );
            return false;
        }
    }

    if(! utils::validate_table_definition(context, *c)) {
        return false;
    }

    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->primary_key());
    if(! utils::validate_index_key_type(context, *i)) {
        return false;
    }

    // Creating sequence can possibly hit cc engine error (esp. with occ),
    // so do it first so that we can exit early in case of errors.
    auto rh = std::any_cast<plan::storage_processor_result>(ct_->runtime_hint());
    if(rh.primary_key_generated()) {
        auto p = rh.primary_key_sequence();
        if(! create_generated_sequence(context, *p)) {
            return false;
        }
    }
    for(auto&& s : rh.generated_sequences()) {
        if(! create_generated_sequence(context, *s)) {
            return false;
        }
    }

    // the creator owns the CONTROL privilege on the newly created table
    auth::authorized_users_action_set new_auth{};
    if(context.req_info().request_source()) {
        if(auto name = context.req_info().request_source()->session_info().username(); name.has_value()) {
            new_auth.add_user_actions(name.value(), auth::action_set{auth::action_kind::control});
        }
    }

    storage::storage_entry tid{};
    std::string serialized{};
    if(! create_primary_storage(context, *i, &new_auth, nullptr, tid, &serialized)) {
        return false;
    }
    if(auto err = recovery::deserialize_storage_option_into_provider(serialized, provider, provider, true)) {
        set_error_info(context, err);
        return false;
    }
    if(! lock_storage_entry(context, tid, c->simple_name())) {
        return false;
    }

    // tx to remember that it has used the storage
    // this is not mandatory for create (since created one is not accessible externally),
    // but to be consistent with drop ddls
    if (context.transaction()) {
        context.transaction()->add_storage_ref(tid);
    }
    return true;
}

}  // namespace jogasaki::executor::common
