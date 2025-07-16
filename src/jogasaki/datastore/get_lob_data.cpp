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
#include "get_lob_data.h"

#include <memory>
#include <string_view>

#include "datastore.h"

#include <tateyama/api/server/blob_info.h>

#include <jogasaki/datastore/blob_info_impl.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/lob/lob_data_provider.h>
#include <jogasaki/lob/lob_id.h>

namespace jogasaki::datastore {

std::uint64_t new_lob_id() noexcept {
    static std::atomic_size_t lob_data_id_src{0};
    return ++lob_data_id_src;
}

status get_lob_data(lob::lob_id_type id, lob::lob_data_provider provider,
                    std::shared_ptr<error::error_info> &error,
                    std::unique_ptr<tateyama::api::server::blob_info> &info) {
    (void) provider; // currently assuming provider is always datastore
    auto *ds = get_datastore();
    auto file = ds->get_blob_file(id);
    if (!file) {
        auto res = status::err_invalid_state;
        error = create_error_info(error_code::lob_reference_invalid,
                            "invalid blob reference", res);
        return res;
    }

    std::string name{"lob-"};
    name += std::to_string(new_lob_id());

    std::filesystem::path path{file.path().string()};
    info = std::make_unique<blob_info_impl>(name, path);

    return status::ok;
}

} // namespace jogasaki::datastore
