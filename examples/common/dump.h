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
#include <jogasaki/utils/watch.h>

namespace jogasaki::common_cli {

void dump_perf_info() {
    auto& watch = utils::get_watch();
    watch.set_point(time_point_main_completed);
#ifdef PERFORMANCE_TOOLS
    auto results = watch.laps(time_point_prepare, time_point_produce);
    for(auto r : *results.get()) {
        LOG(INFO) << "prepare\t" << r << " ms" ;
    }
    results = watch.laps(time_point_produce, time_point_produced);
    for(auto r : *results.get()) {
        LOG(INFO) << "produce\t" << r << " ms" ;
    }
    results = watch.laps(time_point_consume, time_point_consumed);
    for(auto r : *results.get()) {
        LOG(INFO) << "consume\t" << r << " ms" ;
    }
#else
    LOG(INFO) << "prepare: total " << watch.duration(time_point_prepare, time_point_produce) << "ms, average " << watch.average_duration(time_point_prepare, time_point_produce) << "ms" ;
    LOG(INFO) << "produce: total " << watch.duration(time_point_produce, time_point_produced) << "ms, average " << watch.average_duration(time_point_produce, time_point_produced) << "ms" ;
    LOG(INFO) << "transfer: total " << watch.duration(time_point_produced, time_point_consume, true) << "ms" ;
    LOG(INFO) << "consume: total " << watch.duration(time_point_consume, time_point_consumed) << "ms, average " << watch.average_duration(time_point_consume, time_point_consumed) << "ms" ;
    LOG(INFO) << "finish: total " << watch.duration(time_point_consumed, time_point_main_completed, true) << "ms" ;
#endif
}

} //namespace

