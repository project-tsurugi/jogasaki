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
#ifdef PERFORMANCE_TOOLS
#include <performance-tools/perf_counter.h>
#include <performance-tools/lap_counter.h>
#else
#include <jogasaki/utils/watch.h>
#endif

namespace jogasaki::utils {

#ifdef PERFORMANCE_TOOLS
using watch_class = performance_tools::lap_counter_class;
#else
using watch_class = jogasaki::utils::watch;
#endif

watch_class& get_watch();
void dump_info(watch_class&, watch_class::point_in_code, watch_class::point_in_code, std::string_view);

} // namespace
