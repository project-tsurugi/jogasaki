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
#include "processor.h"

namespace jogasaki::executor::process::impl {

processor::processor(std::shared_ptr<processor_info> info) noexcept:
    info_(std::move(info))
{}
abstract::status processor::run(abstract::task_context *context) {
    (void) context;
    impl::relop::engine visitor{const_cast<graph::graph<relation::expression>&>(info_->operators()), {}, {}};
    return abstract::status::completed;
}

}
