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

#ifdef TRACY_ENABLE
#include "../third_party/tracy/Tracy.hpp"
#include "../third_party/tracy/common/TracySystem.hpp"
#define trace_scope ZoneScoped
#define trace_scope_name(name) ZoneScopedN(name)

#else
#define trace_scope
#define trace_scope_name(name)
#endif
