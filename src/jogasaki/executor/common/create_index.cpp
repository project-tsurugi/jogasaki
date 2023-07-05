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
#include "create_index.h"

#include <yugawara/binding/extract.h>

#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/storage_options.h>

namespace jogasaki::executor::common {

create_index::create_index(
    takatori::statement::create_index& ct
) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind create_index::kind() const noexcept {
    return model::statement_kind::create_index;
}

bool create_index::operator()(request_context& context) const {
    (void)context;
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    auto& provider = *context.storage_provider();
    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->definition());
    if(provider.find_index(i->simple_name())) {
        VLOG_LP(log_error) << "Index " << i->simple_name() << " already exists.";
        context.status_code(status::err_already_exists);
        return false;
    }

    std::string storage{};
    if(auto res = recovery::create_storage_option(*i, storage, utils::metadata_serializer_option{false}); ! res) {
        context.status_code(status::err_already_exists);
        return res;
    }
    auto target = std::make_shared<yugawara::storage::configurable_provider>();
    if(auto res = recovery::deserialize_storage_option_into_provider(storage, provider, *target, false); ! res) {
        context.status_code(status::err_unknown);
        return false;
    }

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(auto stg = context.database()->create_storage(i->simple_name(), options);! stg) {
        VLOG_LP(log_info) << "storage " << i->simple_name() << " already exists ";
        // something went wrong. Storage already exists. // TODO recreate storage with new storage option
        VLOG_LP(log_warning) << "storage " << i->simple_name() << " already exists ";
        context.status_code(status::err_unknown);
        return false;
    }
    // only after successful update for kvs, merge metadata
    recovery::merge_deserialized_storage_option(*target, provider, true);
    return true;
}

}
