/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "source.h"

#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/exchange/mock/aggregate/input_partition.h>
#include <jogasaki/executor/exchange/mock/aggregate/shuffle_info.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/request_context.h>

#include "reader.h"

namespace jogasaki::executor::exchange::mock::aggregate {

source::source() = default;
source::~source() = default;

source::source(std::shared_ptr<shuffle_info> info,
        request_context* context
) : info_(std::move(info)), context_(context) {
    (void)context_;
}

void source::receive(std::unique_ptr<input_partition> in) {
    partitions_.emplace_back(std::move(in));
}

io::reader_container source::acquire_reader() {
    return io::reader_container(
        readers_.emplace_back(
            std::make_unique<reader>(info_, partitions_, *info_->aggregator())
        ).get()
    );
}

}
