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
#include "index.h"

#include <yugawara/binding/extract.h>
#include <takatori/util/fail.h>

#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::recovery {

bool deserialize_into_provider(
    proto::metadata::storage::IndexDefinition const& idef,
    yugawara::storage::configurable_provider const& src,
    yugawara::storage::configurable_provider& target
) {
    utils::storage_metadata_serializer ser{};
    std::shared_ptr<yugawara::storage::configurable_provider> deserialized{};
    if(! ser.deserialize(idef, src, deserialized)) {
        VLOG(log_error) << "deserialization error";
        return false;
    }

    std::shared_ptr<yugawara::storage::index const> idx{};
    std::size_t cnt = 0;
    deserialized->each_index([&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
        idx = entry;
        ++cnt;
    });
    if(cnt != 1) {
        VLOG(log_error) << "deserialization error: too many indices";
        return false;
    }

    std::vector<std::shared_ptr<yugawara::storage::sequence const>> sequences{};
    deserialized->each_sequence([&](std::string_view, std::shared_ptr<yugawara::storage::sequence const> const& entry) {
        sequences.emplace_back(entry);
    });
    for(auto&& s : sequences) {
        deserialized->remove_sequence(s->simple_name());
        try {
            target.add_sequence(s, true);
        } catch(std::invalid_argument& e) {
            VLOG(log_error) << "sequence " << s->simple_name() << " already exists";
            return false;
        }
    }

    deserialized->remove_relation(idx->shared_table()->simple_name());
    try {
        target.add_table(idx->shared_table(), true);
    } catch(std::invalid_argument& e) {
        VLOG(log_error) << "table " << idx->shared_table()->simple_name() << " already exists";
        return false;
    }

    deserialized->remove_index(idx->simple_name());
    try {
        target.add_index(idx, false);
    } catch(std::invalid_argument& e) {
        VLOG(log_error) << "primary index " << idx->simple_name() << " already exists";
        return false;
    }
    return true;
}

bool serialize_index(const yugawara::storage::index &i, proto::metadata::storage::IndexDefinition &idef) {
    idef = {};
    utils::storage_metadata_serializer ser{};
    if(! ser.serialize(i, idef)) {
        VLOG(log_error) << "serialization error";
        return false;
    }
    return true;
}

}
