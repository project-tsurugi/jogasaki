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

#include <cstdint>
#include <optional>

#include <jogasaki/lob/lob_id.h>

namespace jogasaki::utils {

/**
 * @brief assign reference tag for the given blob id
 * @param transaction_id the transaction id (surrogate id)
 * @param blob_id the blob id
 * @return the generated reference tag, or std::nullopt if the operation failed
 */
std::optional<lob::lob_reference_tag_type> assign_reference_tag(
    std::uint64_t transaction_id,
    lob::lob_id_type blob_id
);

}  // namespace jogasaki::utils
