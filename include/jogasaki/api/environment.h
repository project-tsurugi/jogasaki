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

namespace jogasaki::api {

/**
 * @brief environment initializer
 * @details this class setup jogasaki client environment.
 * The client should initialize this object and keep it as long as jogasaki runs.
 */
class environment {
public:
    environment() = default;
    virtual ~environment() = default;
    environment(environment const& other) = delete;
    environment(environment&& other) = delete;
    environment& operator=(environment const& other) = delete;
    environment& operator=(environment&& other) = delete;

    /**
     * @brief initialize the environment in order to initialize static control area (e.g. one held by glog).
     * The client code must create environment instance, call initialize() function,
     * and keep it until the client completes interaction with jogasaki. Typically this can be done in main() of the client.
     */
    virtual void initialize() = 0;
};

/**
 * @brief factory method for environment
 * @return Environment for the current implementation
 */
std::unique_ptr<environment> create_environment();

}