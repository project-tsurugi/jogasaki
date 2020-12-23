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

#include <glog/logging.h>
#include <jogasaki/utils/performance_tools.h>
#include "show_producer_perf_info.h"

namespace jogasaki::common_cli {

void show_perf_info() {
    auto& watch = utils::get_watch();
    watch.set_point(time_point_main_completed);
    show_producer_perf_info();
#ifndef PERFORMANCE_TOOLS
    LOG(INFO) << "transfer: total " << watch.duration(time_point_produced, time_point_consume, true) << "ms" ;
#endif
    LOG(INFO) << jogasaki::utils::textualize(watch, time_point_consume, time_point_consumed, "consume");
#ifndef PERFORMANCE_TOOLS
    LOG(INFO) << "finish: total " << watch.duration(time_point_consumed, time_point_main_completed, true) << "ms" ;
#endif
}

} //namespace
