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
#include "storage_options.h"

#include <yugawara/storage/configurable_provider.h>
#include <takatori/util/fail.h>

#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/proto_debug_string.h>

#include <jogasaki/proto/metadata/storage.pb.h>

#include "index.h"

namespace jogasaki::recovery {

bool create_storage_option(
    yugawara::storage::index const&i,
    std::string &storage,
    utils::metadata_serializer_option const& option
) {
    storage.clear();
    proto::metadata::storage::IndexDefinition idef{};

    if(! recovery::serialize_index(i, idef, option)) {
        return false;
    }
    proto::metadata::storage::Storage stg{};
    stg.set_message_version(metadata_format_version);
    stg.set_allocated_index(&idef);

    std::stringstream ss{};
    if (!stg.SerializeToOstream(&ss)) {
        return false;
    }
    storage = ss.str();
    VLOG_LP(log_trace) << "storage_option:" << utils::to_debug_string(stg);
    stg.release_index();
    return true;
}

bool validate_extract(std::string_view payload, proto::metadata::storage::IndexDefinition& out) {
    proto::metadata::storage::Storage st{};
    if (! st.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
        VLOG_LP(log_error) << "Invalid metadata data is detected in the storage.";
        return false;
    }
    if(st.message_version() != metadata_format_version) {
        VLOG_LP(log_error) << "Incompatible metadata version (" << st.message_version() <<
            ") is stored in the storage. This version is not supported.";
        return false;
    }
    if(st.has_index()) {
        out = st.index();
    }
    return true;
}

bool deserialize_storage_option_into_provider(
    std::string_view payload,
    yugawara::storage::configurable_provider const &src,
    yugawara::storage::configurable_provider& target,
    bool overwrite
) {
    proto::metadata::storage::IndexDefinition idef{};
    if(! recovery::validate_extract(payload, idef)) {
        return false;
    }
    if(! recovery::deserialize_into_provider(idef, src, target, overwrite)) {
        return false;
    }
    return true;
}

bool merge_deserialized_storage_option(
    yugawara::storage::configurable_provider& src,
    yugawara::storage::configurable_provider& target,
    bool overwrite
) {
    std::shared_ptr<yugawara::storage::index const> idx{};
    std::size_t cnt = 0;
    src.each_index([&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
        idx = entry;
        ++cnt;
    });
    if(cnt != 1) {
        VLOG_LP(log_error) << "deserialization error: too many indices";
        return false;
    }

    std::vector<std::shared_ptr<yugawara::storage::sequence const>> sequences{};
    src.each_sequence([&](std::string_view, std::shared_ptr<yugawara::storage::sequence const> const& entry) {
        sequences.emplace_back(entry);
    });
    for(auto&& s : sequences) {
        src.remove_sequence(s->simple_name());
        try {
            target.add_sequence(s, overwrite);
        } catch(std::invalid_argument& e) {
            VLOG_LP(log_error) << "sequence " << s->simple_name() << " already exists";
            return false;
        }
    }

    src.remove_relation(idx->shared_table()->simple_name());
    try {
        target.add_table(idx->shared_table(), overwrite);
    } catch(std::invalid_argument& e) {
        VLOG_LP(log_error) << "table " << idx->shared_table()->simple_name() << " already exists";
        return false;
    }

    src.remove_index(idx->simple_name());
    try {
        target.add_index(idx, overwrite);
    } catch(std::invalid_argument& e) {
        VLOG_LP(log_error) << "primary index " << idx->simple_name() << " already exists";
        return false;
    }
    return true;
}

}
