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

#include <jogasaki/scheduler/thread_info.h>

namespace jogasaki::scheduler {

/**
 * @brief thread specific information to identify the worker thread
 */
inline thread_local thread_info thread_local_info_{};  //NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

}  // namespace jogasaki::scheduler
