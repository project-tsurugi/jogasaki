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

#include <executor/exchange/source.h>
#include <executor/exchange/group/shuffle_info.h>
#include <executor/exchange/group/input_partition.h>
#include "executor/group_reader.h"

namespace dc::executor::exchange::deliver {

class reader;

class source : public exchange::source {
public:
    source();
    ~source();
    source(source&& other) noexcept = delete;
    source& operator=(source&& other) noexcept = delete;
    reader_container acquire_reader() override;

private:
};

}
