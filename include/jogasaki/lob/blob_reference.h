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

#pragma once

#include <jogasaki/lob/lob_reference.h>

namespace jogasaki::lob {

/**
 * @brief blob field data object
 * @details Trivially copyable immutable class holding lob reference.
 */
class blob_reference : public lob_reference {
public:
    /**
     * @brief default constructor representing empty object
     */
    constexpr blob_reference() = default;

    /**
     * @brief construct `provided` object
     * @param locator the locator of the lob data
     */
    explicit blob_reference(lob_locator const& locator) :
        lob_reference(locator)
    {}

    /**
     * @brief construct `fetched` object
     * @param id the lob reference id
     */
    explicit blob_reference(lob_id_type id) :
        lob_reference(id)
    {}

    /**
     * @brief construct `resolved` object
     * @param id lob reference id
     * @param provider the provider that gives the lob data
     */
    blob_reference(lob_id_type id, lob_data_provider provider) :
        lob_reference(id, provider)
    {}

};

}  // namespace jogasaki::lob
