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
#pragma once

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/kvs/storage.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief scan info
 * @details this instance provides specification of scan (e.g. definition of the range of scanned records)
 */
class scan_info : public abstract::scan_info {
public:
    /**
     * @brief create new object
     */
    explicit scan_info(
        std::string_view begin_key = {},
        kvs::end_point_kind begin_endpoint = kvs::end_point_kind::unbound,
        std::string_view end_key = {},
        kvs::end_point_kind end_endpoint = kvs::end_point_kind::unbound
    );

    ~scan_info() override = default;

    scan_info(scan_info const& other) = default;
    scan_info& operator=(scan_info const& other) = default;
    scan_info(scan_info&& other) noexcept = default;
    scan_info& operator=(scan_info&& other) noexcept = default;

    [[nodiscard]] std::string_view begin_key() const noexcept;
    [[nodiscard]] std::string_view end_key() const noexcept;
    [[nodiscard]] kvs::end_point_kind begin_endpoint() const noexcept;
    [[nodiscard]] kvs::end_point_kind end_endpoint() const noexcept;

private:
    data::aligned_buffer begin_key_{};
    kvs::end_point_kind begin_endpoint_{};
    data::aligned_buffer end_key_{};
    kvs::end_point_kind end_endpoint_{};
};

}


