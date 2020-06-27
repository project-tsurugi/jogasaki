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

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/record_writer.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

template <kind ... Types>
class basic_record_writer : public executor::record_writer {
public:

    using record_type = basic_record<Types...>;
    using records_type = std::vector<record_type, std::allocator<record_type>>;

    basic_record_writer() = default;

    bool write(accessor::record_ref rec) override {
        records_.emplace_back(record_type(rec));
        return false;
    }

    void flush() override {
        // no-op
    }

    void release() override {
        released_ = true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return records_.size();
    }

private:
    records_type records_{};
    bool released_{false};
};

using record_writer = basic_record_writer<kind::int8, kind::float8>;

template <kind ... Types, typename T = std::enable_if<sizeof...(Types) != 0, void>>
basic_record_writer<Types...>* unwrap(executor::record_writer* writer) {
    return static_cast<basic_record_writer<Types...>*>(writer);
}

inline record_writer* unwrap_record_writer(executor::record_writer* writer) {
    return unwrap<kind::int8, kind::float8>(writer);
}

}

