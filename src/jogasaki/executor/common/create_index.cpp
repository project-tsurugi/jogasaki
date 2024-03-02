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
#include "create_index.h"

#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/abort_transaction.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

namespace jogasaki::executor::common {

using takatori::util::string_builder;

create_index::create_index(
    takatori::statement::create_index& ct
) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind create_index::kind() const noexcept {
    return model::statement_kind::create_index;
}

// return false if table is not empty, or error with kvs
bool create_index::validate_empty_table(request_context& context, std::string_view table_name) const {
    auto stg = context.database()->get_or_create_storage(table_name);
    std::unique_ptr<kvs::iterator> it{};
    if(auto res =
           stg->content_scan(*context.transaction(), {}, kvs::end_point_kind::unbound, {}, kvs::end_point_kind::unbound, it);
       res != status::ok) {
        handle_kvs_errors(context, res);
        handle_generic_error(context, res, error_code::sql_execution_exception);
        return false;
    }
    auto st = it->next();
    if(st == status::ok) {
        set_error(
            context,
            error_code::unsupported_runtime_feature_exception,
            string_builder{} << "Records exist in the table \"" << table_name
                             << "\" and creating index is not supported for tables with existing records"
                             << string_builder::to_string,
            status::err_unsupported
        );
        it.reset();
        utils::abort_transaction(*context.transaction());
        return false;
    }
    if(st == status::not_found) {
        return true;
    }
    handle_kvs_errors(context, st);
    return false;
}

bool create_index::operator()(request_context& context) const {
    (void)context;
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    auto& provider = *context.storage_provider();
    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->definition());
    if(provider.find_index(i->simple_name())) {
        set_error(
            context,
            error_code::target_already_exists_exception,
            string_builder{} << "Index \"" << i->simple_name() << "\" already exists." << string_builder::to_string,
            status::err_already_exists
        );
        return false;
    }
    if(! validate_empty_table(context, i->table().simple_name())) {
        return false;
    }

    std::string storage{};
    if(auto err = recovery::create_storage_option(*i, storage, utils::metadata_serializer_option{false})) {
        set_error_info(context, err);
        return false;
    }
    auto target = std::make_shared<yugawara::storage::configurable_provider>();
    if(auto err = recovery::deserialize_storage_option_into_provider(storage, provider, *target, false)) {
        set_error_info(context, err);
        return false;
    }

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage(i->simple_name(), options);! stg) {
        // something went wrong. Storage already exists. // TODO recreate storage with new storage option
        VLOG_LP(log_warning) << "storage " << i->simple_name() << " already exists ";
        set_error(
            context,
            error_code::sql_execution_exception,
            string_builder{} << "Unexpected error." << string_builder::to_string,
            status::err_unknown
        );
        return false;
    }
    // only after successful update for kvs, merge metadata
    if(auto err = recovery::merge_deserialized_storage_option(*target, provider, true)) {
        if(! VLOG_IS_ON(log_trace)) {  // avoid duplicate log entry with log_trace
            VLOG_LP(log_error) << "error_info:" << *err;
        }
        return false;
    }
    return true;
}

}  // namespace jogasaki::executor::common
