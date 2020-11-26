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

#include <jogasaki/mock/basic_record.h>

namespace jogasaki::test {

using kind = meta::field_type_kind;
using takatori::util::maybe_shared_ptr;
using takatori::util::fail;
using jogasaki::mock::basic_record;
using jogasaki::mock::create_record;

class record : public basic_record {
public:
    using key_type = std::int64_t;
    using value_type = double;

    record() noexcept : basic_record(create_record<kind::int8, kind::float8>(0, 0.0)) {}

    record(key_type key, value_type value) : basic_record(basic_record(create_record<kind::int8, kind::float8>(key, value))) {}

    [[nodiscard]] key_type key() const noexcept {
        return ref().get_value<key_type>(meta()->value_offset(0));
    }

    void key(key_type arg) noexcept {
        ref().set_value<key_type>(meta()->value_offset(0), arg);
    }

    [[nodiscard]] value_type value() const noexcept {
        return ref().get_value<value_type>(meta()->value_offset(1));
    }

    void value(value_type arg) noexcept {
        ref().set_value<value_type>(meta()->value_offset(1), arg);
    }
};

static_assert(sizeof(record) > record::buffer_size);
static_assert(alignof(record) == 8);
static_assert(!std::is_trivially_copyable_v<record>);

class record_f4f8ch : public basic_record {
public:
    using key_type = std::int32_t;
    using f4_value_type = double;
    using ch_value_type = meta::field_type_traits<kind::character>::runtime_type;

    record_f4f8ch() noexcept : basic_record(create_record<kind::float8, kind::int4, kind::character>(0.0, 0, ch_value_type{})) {}

    record_f4f8ch(f4_value_type f4_value, key_type key, ch_value_type ch_value) :
        basic_record(create_record<kind::float8, kind::int4, kind::character>(f4_value, key, ch_value))
    {}

    [[nodiscard]] key_type key() const noexcept {
        return ref().get_value<key_type>(meta()->value_offset(1));
    }

    [[nodiscard]] f4_value_type f4_value() const noexcept {
        return ref().get_value<f4_value_type>(meta()->value_offset(0));
    }

    [[nodiscard]] ch_value_type ch_value() const noexcept {
        return ref().get_value<ch_value_type>(meta()->value_offset(2));
    }
};

}
