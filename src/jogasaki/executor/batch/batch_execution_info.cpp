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
#include "batch_execution_info.h"

#include <cstddef>
#include <string>

namespace jogasaki::executor::batch {

batch_execution_info::batch_execution_info(
    api::statement_handle prepared,
    maybe_shared_ptr<const api::parameter_set> parameters,
    api::impl::database *db,
    std::function<void(void)> cb,
    batch_executor_option opt
) noexcept:
    prepared_(prepared),
    parameters_(std::move(parameters)),
    db_(db),
    completion_callback_(std::move(cb)),
    options_(std::move(opt))
{}

api::statement_handle batch_execution_info::prepared() const noexcept {
    return prepared_;
}

const maybe_shared_ptr<const api::parameter_set> &batch_execution_info::parameters() const noexcept {
    return parameters_;
}

api::impl::database *batch_execution_info::db() const noexcept {
    return db_;
}

batch_execution_info::completion_callback_type batch_execution_info::completion_callback() const noexcept {
    return completion_callback_;
}

batch_executor_option const &batch_execution_info::options() const noexcept {
    return options_;
}

}