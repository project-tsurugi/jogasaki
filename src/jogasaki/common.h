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

// common things to all translation unit

#include <tateyama/common.h>

#ifdef LIKWID_PERFMON
#include "likwid-marker.h"
#else

#ifdef PERFORMANCE_TOOLS

#include "performance-tools/perf_counter.h"
#include "performance-tools/marker.h"

#define LIKWID_MARKER_INIT MARKER_INIT
#define LIKWID_MARKER_START(regionTag) MARKER_START(regionTag)
#define LIKWID_MARKER_STOP(regionTag) MARKER_STOP(regionTag)
#define LIKWID_MARKER_CLOSE MARKER_CLOSE

#ifdef trace_scope_name
#undef trace_scope_name
#endif
#define trace_scope_name(regionTag) MARKER_SCOPE(regionTag)

#endif  // PERFORMANCE_TOOLS

#endif  // LIKWID_PERFMON
