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

#include <jogasaki/status.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/lob/lob_reference.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::datastore {

/**
 * @brief register lob file and publish new id and reference tag
 * @param path the path for the lob file
 * @param tx transaction to keep the scope object (blob pool) for the lob data
 * @param out [out] blob id assigned for the input lob data
 * @param reference_tag [out] blob reference tag assigned for the input lob data
 * @param error [out] error information is set when status code other than status::ok is returned
 * @return status::ok when successful
 * @return any other error otherwise
 */
status assign_lob_id(
    lob::lob_reference const& ref,
    transaction_context* tx,
    lob::lob_id_type& id,
    lob::lob_reference_tag_type& reference_tag,
    std::shared_ptr<error::error_info>& error
);

}  // namespace jogasaki::datastore
