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

namespace jogasaki::executor::process::abstract {

/**
 * @brief scan info
 * @details this instance provides specification of scan (e.g. definition of the range of scanned records)
 */
class scan_info {
public:
    /**
     * @brief create empty object
     */
    scan_info() = default;

    /**
     * @brief destroy the object
     */
    virtual ~scan_info() = default;

    scan_info(scan_info const& other) = default;
    scan_info& operator=(scan_info const& other) = default;
    scan_info(scan_info&& other) noexcept = default;
    scan_info& operator=(scan_info&& other) noexcept = default;
};

}


