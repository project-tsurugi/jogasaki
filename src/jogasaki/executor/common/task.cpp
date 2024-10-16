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
#include "task.h"

#include <atomic>
#include <ostream>
#include <string>
#include <string_view>

#include <jogasaki/model/task.h>

namespace jogasaki::executor::common {

task::task() :
    id_(id_src++)
{}

task::task(request_context* context, step_type* src) :
    id_(id_src++),
    context_(context),
    src_(src)
{}

task::identity_type task::id() const {
    return id_;
}

task::step_type* task::step() const {
    return src_;
}

request_context* task::context() const {
    return context_;
}

std::ostream& task::write_to(std::ostream& out) const {
    using namespace std::string_view_literals;
    return out << "task[id="sv << std::to_string(static_cast<identity_type>(id_)) << "]"sv;
}

bool task::has_transactional_io() {
    return false;
}

void task::dump(std::ostream& out, int indent) const {
    std::string indent_space(indent, ' ');

    out << indent_space << "model::task dump:" << std::endl;
    out << indent_space << "  common::task dump:" << std::endl;
    out << indent_space << "    id: " << id_ << std::endl;
    out << indent_space << "    context: " << context_ << std::endl;
    context_->dump(out,6);
    out << indent_space << "    step: " << src_ << std::endl;
}
}


