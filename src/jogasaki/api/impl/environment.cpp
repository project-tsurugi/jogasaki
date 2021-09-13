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
#include "environment.h"

#include <memory>

namespace jogasaki::api {
/**
 * @brief factory method for environment
 * @return Environment for the current implementation
 */
std::unique_ptr<environment> create_environment() {
    return std::make_unique<impl::environment>();
}

void impl::environment::initialize() {
    environment_.initialize();
}
}

extern "C" jogasaki::api::environment* new_environment() {
    return new jogasaki::api::impl::environment();
}

extern "C" void delete_environment(jogasaki::api::environment* env) {
    delete env;  //NOLINT
}
