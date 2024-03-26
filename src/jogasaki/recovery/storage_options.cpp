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
#include "storage_options.h"

#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <vector>
#include <glog/logging.h>

#include <takatori/util/string_builder.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>

#include <jogasaki/constants.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/proto_debug_string.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include "index.h"

namespace jogasaki::recovery {

using takatori::util::string_builder;

std::shared_ptr<error::error_info> create_storage_option(
    yugawara::storage::index const& idx,
    std::string& out,
    utils::metadata_serializer_option const& option
) {
    out.clear();
    proto::metadata::storage::IndexDefinition idef{};

    if(auto err = recovery::serialize_index(idx, idef, option)) {
        return err;
    }
    proto::metadata::storage::Storage stg{};
    stg.set_message_version(metadata_format_version);
    stg.set_allocated_index(&idef);

    std::stringstream ss{};
    if (!stg.SerializeToOstream(&ss)) {
        return create_error_info(
            error_code::sql_execution_exception,
            "creating storage option failed",
            status::err_unknown
        );
    }
    out = ss.str();
    VLOG_LP(log_trace) << "storage_option:" << utils::to_debug_string(stg);
    (void)stg.release_index();
    return {};
}

std::shared_ptr<error::error_info>
validate_extract(std::string_view payload, proto::metadata::storage::IndexDefinition& out) {
    proto::metadata::storage::Storage st{};
    if (! st.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
        return create_error_info(
            error_code::sql_execution_exception,
            "invalid metadata detected in the storage",
            status::err_unknown
        );
    }
    if(st.message_version() != metadata_format_version) {
        return create_error_info(
            error_code::sql_execution_exception,
            string_builder{} << "Incompatible metadata version (" << st.message_version() <<
                ") is stored in the storage. This version is not supported." << string_builder::to_string,
            status::err_unknown
        );
    }
    if(st.has_index()) {
        out = st.index();
    }
    return {};
}

std::shared_ptr<error::error_info> deserialize_storage_option_into_provider(
    std::string_view payload,
    yugawara::storage::configurable_provider const &src,
    yugawara::storage::configurable_provider& out,
    bool overwrite
) {
    proto::metadata::storage::IndexDefinition idef{};
    if(auto err = recovery::validate_extract(payload, idef)) {
        return err;
    }
    if(auto err = recovery::deserialize_into_provider(idef, src, out, overwrite)) {
        return err;
    }
    return {};
}

std::shared_ptr<error::error_info> merge_deserialized_storage_option(
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
        return create_error_info(
            error_code::sql_execution_exception,
            "deserialization error: too many indices",
            status::err_unknown
        );
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
            return create_error_info(
                error_code::target_already_exists_exception,
                string_builder{} << "sequence \"" << s->simple_name() << "\" already exists" << string_builder::to_string,
                status::err_already_exists
            );
        }
    }

    src.remove_relation(idx->shared_table()->simple_name());
    try {
        target.add_table(idx->shared_table(), overwrite);
    } catch(std::invalid_argument& e) {
        return create_error_info(
            error_code::target_already_exists_exception,
            string_builder{} << "table \"" << idx->shared_table()->simple_name() << "\" already exists" << string_builder::to_string,
            status::err_already_exists
        );
    }

    src.remove_index(idx->simple_name());
    try {
        target.add_index(idx, overwrite);
    } catch(std::invalid_argument& e) {
        return create_error_info(
            error_code::target_already_exists_exception,
            string_builder{} << "primary index \"" << idx->simple_name() << "\" already exists" << string_builder::to_string,
            status::err_already_exists
        );
    }
    return {};
}

}
