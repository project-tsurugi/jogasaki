/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <memory>
#include <string>

#include <jogasaki/model/task.h>

namespace jogasaki { class request_context; }
namespace jogasaki::executor::process { class step; class processor_info; }
namespace jogasaki::dist {
// Development selection is explicit; native location resolution will supply this binding.
[[nodiscard]] std::shared_ptr<model::task> make_remote_process_task(
    request_context& context, executor::process::step& step,
    executor::process::processor_info const& info, std::string server);
}
