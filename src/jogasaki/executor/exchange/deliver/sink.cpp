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
#include "sink.h"

#include <jogasaki/executor/io/record_writer.h>

#include "writer.h"

namespace jogasaki::executor::exchange::deliver {

sink::sink() noexcept : writer_(std::make_unique<writer>()) {}

io::record_writer& sink::acquire_writer() {
    return *writer_;
}

void sink::release_writer(io::record_writer& writer) {
    (void)writer;
}

}
