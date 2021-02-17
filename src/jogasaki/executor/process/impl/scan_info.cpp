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
#include "scan_info.h"

namespace jogasaki::executor::process::impl {

scan_info::scan_info(
    std::string_view begin_key,
    kvs::end_point_kind begin_endpoint,
    std::string_view end_key,
    kvs::end_point_kind end_endpoint
) :
    begin_key_(begin_key),
    begin_endpoint_(begin_endpoint),
    end_key_(end_key),
    end_endpoint_(end_endpoint)
{}

std::string_view scan_info::begin_key() const noexcept {
    return begin_key_;
}

std::string_view scan_info::end_key() const noexcept {
    return end_key_;
}

kvs::end_point_kind scan_info::begin_endpoint() const noexcept {
    return begin_endpoint_;
}

kvs::end_point_kind scan_info::end_endpoint() const noexcept {
    return end_endpoint_;
}

}
