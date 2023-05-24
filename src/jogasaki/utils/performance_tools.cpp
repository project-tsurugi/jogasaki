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
#include "performance_tools.h"

#include <memory>
#include <sstream>

namespace jogasaki::utils {

watch_class& get_watch() {
    static std::unique_ptr<watch_class> watch_ = std::make_unique<watch_class>();
    return *watch_;
}

std::string textualize(
    watch_class& result,
    watch_class::point_in_code bgn,
    watch_class::point_in_code end,
    std::string_view label
) {
    std::stringstream ss;
    ss << label << ": total " << result.duration(bgn, end) << "ms, average " << result.average_duration(bgn, end) << "ms" ;
    return ss.str();
}

} // namespace
