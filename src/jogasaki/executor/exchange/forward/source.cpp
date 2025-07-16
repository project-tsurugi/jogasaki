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
#include "reader.h"

#include <jogasaki/executor/io/reader_container.h>

namespace jogasaki::executor::exchange::forward {

source::source() = default;
source::~source() = default;

source::source(
    std::shared_ptr<forward_info> info,
    request_context* context,
    std::shared_ptr<input_partition> partition
) :
    info_(std::move(info)),
    context_(context),
    partition_(std::move(partition))
{}

io::reader_container source::acquire_reader() {
    if (! reader_) {
        reader_ = std::make_unique<reader>(info_, partition_);
    }
    return io::reader_container{reader_.get()};
}

}  // namespace jogasaki::executor::exchange::forward
