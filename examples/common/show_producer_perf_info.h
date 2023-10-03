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

#include <glog/logging.h>
#include <jogasaki/utils/performance_tools.h>
#include "producer_constants.h"

namespace jogasaki::common_cli {

void show_producer_perf_info() {
    auto& watch = utils::get_watch();
    LOG(INFO) << jogasaki::utils::textualize(watch, time_point_prepare, time_point_prepared, "prepare");
    LOG(INFO) << jogasaki::utils::textualize(watch, time_point_prepared, time_point_touched, "first touch");
    LOG(INFO) << jogasaki::utils::textualize(watch, time_point_touched, time_point_produce, "wait others prepare");
    LOG(INFO) << jogasaki::utils::textualize(watch, time_point_produce, time_point_produced, "produce");
}

} //namespace
