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
#ifdef PERFORMANCE_TOOLS
#include <performance-tools/marker_init.h>  // exactly the same as performance-tools/lap_counter_init.h
#endif

#include <memory>
#include <sstream>

namespace jogasaki::utils {

#ifdef PERFORMANCE_TOOLS

watch_class& get_watch() {
    return performance_tools::get_watch();
}

std::string textualize(watch_class& result, watch_class::point_in_code bgn, watch_class::point_in_code end, std::string_view label) {
    std::stringstream ss;

    ss << "performance counter result for " << label << std::endl;
    auto results = result.laps(bgn, end);
    for(auto r : *results.get()) {
        ss << label << "\t" << r << std::endl;
    }
    return ss.str();
}

#else

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

#endif

} // namespace
