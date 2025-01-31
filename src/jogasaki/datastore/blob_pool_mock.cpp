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
#include "blob_pool_mock.h"

#include <glog/logging.h>

#include <jogasaki/datastore/datastore_mock.h>

namespace jogasaki::datastore {

blob_pool_mock::blob_pool_mock(datastore_mock *parent) : parent_(parent) {}

void blob_pool_mock::release() {
    // no-op
}

limestone::api::blob_id_type blob_pool_mock::register_file(boost::filesystem::path const &file, bool is_temporary_file) {
    (void) is_temporary_file;
    if (parent_->path_to_id_.count(file.string()) != 0) {
        LOG(ERROR) << "file already registered at path:" << file.string();
    }
    auto id = static_cast<limestone::api::blob_id_type>(parent_->id_src_++);
    parent_->path_to_id_[file.string()] = id;
    parent_->id_to_path_[id] = file.string();
    return id;
}

limestone::api::blob_id_type blob_pool_mock::register_data(std::string_view data) {
    // not yet implemented
    (void) data;
    return {};
}

limestone::api::blob_id_type blob_pool_mock::duplicate_data(limestone::api::blob_id_type reference) {
    // not yet implemented
    (void) reference;
    return {};
}

}  // namespace jogasaki::datastore
