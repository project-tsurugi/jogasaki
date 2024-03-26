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
#include "reader_container.h"

#include <utility>
#include <variant>

#include <takatori/util/fail.h>

#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/record_reader.h>

namespace jogasaki::executor::io {

using takatori::util::fail;

reader_container::reader_container(record_reader* reader) noexcept :
    reader_(std::in_place_type<record_reader*>, reader)
{}

reader_container::reader_container(group_reader* reader) noexcept :
    reader_(std::in_place_type<group_reader*>, reader)
{}

reader_kind reader_container::kind() const noexcept {
    switch(reader_.index()) {
        case index_of<std::monostate>:
            return reader_kind::unknown;
        case index_of<record_reader*>:
            return details::to_kind<record_reader>;
        case index_of<group_reader*>:
            return details::to_kind<group_reader>;
    }
    fail();
}

reader_container::operator bool() const noexcept {
    switch(reader_.index()) {
        case index_of<std::monostate>:
            return false;
        case index_of<record_reader*>:
            return *std::get_if<record_reader*>(&reader_) != nullptr;
        case index_of<group_reader*>:
            return *std::get_if<group_reader*>(&reader_) != nullptr;
    }
    fail();
}

void reader_container::release() {
    if (!*this) return;
    switch(reader_.index()) {
        case index_of<std::monostate>:
            return;
        case index_of<record_reader*>:
            std::get<record_reader*>(reader_)->release();
            return;
        case index_of<group_reader*>:
            std::get<group_reader*>(reader_)->release();
            return;
    }
    fail();
}

}  // namespace jogasaki::executor::io
