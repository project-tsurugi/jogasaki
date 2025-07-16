/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "convert_offset_string.h"

#include <memory>
#include <string>
#include <string_view>
#include <glog/logging.h>

#include <takatori/datetime/conversion.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki::utils {

bool convert_offset_string(std::string_view offset_str, std::int32_t& offset_min) {
    if(offset_str.empty()) {
        offset_min = 0;
        return true;
    }
    auto res = takatori::datetime::parse_zone_offset(offset_str);
    if(res.is_error()) {
        LOG_LP(ERROR) << "invalid value specified for session.zone_offset:" << offset_str << " message:\""
                      << res.error() << "\"";
        return false;
    }
    auto& v = res.value();
    offset_min = (v.plus ? 1 : -1) * (static_cast<std::int32_t>(v.hour) * 60 + static_cast<std::int32_t>(v.minute));
    return true;
}

}  // namespace jogasaki::utils
