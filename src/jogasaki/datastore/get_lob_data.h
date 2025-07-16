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

#include <tateyama/api/server/blob_info.h>

#include <jogasaki/lob/lob_id.h>
#include <jogasaki/lob/lob_data_provider.h>
#include <jogasaki/error/error_info.h>

namespace jogasaki::datastore {

/**
 * @brief request lob data from the provider
 * @param id the blob id to request the lob data
 * @param provider the provider of the requested lob data
 * @param error [out] error information is set when status code other than status::ok is returned
 * @param info [out] the blob information
 * @return status::ok when successful
 * @return any other error otherwise
 */
status get_lob_data(
    lob::lob_id_type id,
    lob::lob_data_provider provider,
    std::shared_ptr<error::error_info>& error,
    std::unique_ptr<tateyama::api::server::blob_info>& info
);

}  // namespace jogasaki::datastore
