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

#include <memory>
#include <vector>

#include <jogasaki/accessor/record_ref.h> // FIXME using internal accessor temporarily
#include <jogasaki/data/iterable_record_store.h>

namespace jogasaki::api {

/**
 * @brief result set interface to provide iterator/disposal method
 * @attention under development
 */
class result_set {
public:
    class impl;
    using iterator = data::iterable_record_store::iterator;

    explicit result_set(std::unique_ptr<impl> i);
    ~result_set();
    result_set(result_set const& other) = delete;
    result_set& operator=(result_set const& other) = delete;
    result_set(result_set&& other) noexcept = delete;
    result_set& operator=(result_set&& other) noexcept = delete;

    iterator begin();
    iterator end();
    void close();
private:
    std::unique_ptr<impl> impl_;
};

}
