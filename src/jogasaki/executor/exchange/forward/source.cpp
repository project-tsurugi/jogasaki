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
#include "source.h"
#include "reader.h"

#include <jogasaki/executor/io/reader_container.h>

namespace jogasaki::executor::exchange::forward {

io::reader_container source::acquire_reader() {
    if (! reader_) {
        reader_ = std::make_unique<reader>();
    }
    return io::reader_container{reader_.get()};
}

}
