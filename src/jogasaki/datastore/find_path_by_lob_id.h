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
#pragma once

#include <memory>
#include <string_view>

#include "datastore.h"

#include <jogasaki/kvs/database.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::datastore {

/**
 * @brief fetch data file path corresponding to the blob id
 * @param id the requested blob id
 * @param path the path for the lob file
 * @param error [out] error information is set when status code other than status::ok is returned
 * @return status::ok when successful
 * @return status::err_invalid_state when the log id is invalid
 * @return status::err_io_error when io error occurred in datastore
 * @return any other error otherwise
 */
status find_path_by_lob_id(
    lob::lob_id_type id,
    std::string& path,
    std::shared_ptr<error::error_info>& error
);


}  // namespace jogasaki::datastore
