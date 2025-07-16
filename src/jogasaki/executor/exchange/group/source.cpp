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

#include <jogasaki/configuration.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/group/input_partition.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/request_context.h>

#include "priority_queue_reader.h"
#include "sorted_vector_reader.h"

namespace jogasaki::executor::exchange::group {

source::source() = default;
source::~source() = default;

source::source(
    std::shared_ptr<group_info> info,
    request_context* context
) :
    info_(std::move(info)),
    context_(context)
{}

void source::receive(std::unique_ptr<input_partition> in) {
    partitions_.emplace_back(std::move(in));
}

io::reader_container source::acquire_reader() {
    if (context_->configuration()->use_sorted_vector()) {
        return io::reader_container(
            readers_.emplace_back(std::make_unique<sorted_vector_reader>(info_, partitions_)).get()
        );
    }
    return io::reader_container(
        readers_.emplace_back(std::make_unique<priority_queue_reader>(info_, partitions_)).get()
    );
}

}  // namespace jogasaki::executor::exchange::group
