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
#include <glog/logging.h>
#include "performance_tools.h"

#include <memory>

namespace jogasaki::utils {

watch_class& get_watch() {
#ifdef PERFORMANCE_TOOLS
    return performance_tools::get_watch();
#else
    static std::unique_ptr<watch_class> watch_ = std::make_unique<watch_class>();
    return *watch_;
#endif
}

#ifdef PERFORMANCE_TOOLS
void dump_info(watch_class& result, watch_class::point_in_code bgn, watch_class::point_in_code end, std::string_view label) {
    auto results = result.laps(bgn, end);
    for(auto r : *results.get()) {
        LOG(INFO) << label << "\t" << r;
    }
}
#else
void dump_info(watch_class& result, watch_class::point_in_code bgn, watch_class::point_in_code end, std::string_view label) {
    LOG(INFO) << label << ": total " << result.duration(bgn, end) << "ms, average " << result.average_duration(bgn, end) << "ms" ;
}
#endif

} // namespace
