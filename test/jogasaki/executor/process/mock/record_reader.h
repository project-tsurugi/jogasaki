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

#include <takatori/util/sequence_view.h>

#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/basic_record.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::process::mock {

using namespace testing;

using kind = meta::field_type_kind;

template <kind ... Types>
class basic_record_reader : public executor::record_reader {
public:
    using record_type = basic_record<Types...>;
    using records_type = std::vector<record_type>;

    basic_record_reader() = default;

    explicit basic_record_reader(records_type records) : records_(std::move(records)) {}

    [[nodiscard]] bool available() const override {
        return it_ != records_.end() && it_+1 != records_.end();
    }

    bool next_record() override {
        if (!initialized_) {
            it_ = records_.begin();
            initialized_ = true;
        } else {
            if (it_ == records_.end()) {
                return false;
            }
            ++it_;
        }
        return it_ != records_.end();
    }

    [[nodiscard]] accessor::record_ref get_record() const {
        return it_->ref();
    }

    void release() override {
        records_.clear();
        released_ = true;
    }

    std::shared_ptr<meta::record_meta> const& meta() {
        static record_type rec{};
        return rec.record_meta();
    }

private:
    records_type records_{};
    bool initialized_{false};
    bool released_{false};
    typename records_type::iterator it_{};
};

using record_reader = basic_record_reader<kind::int8, kind::float8>;

template <kind ... Types, typename T = std::enable_if<sizeof...(Types) != 0, executor::record_reader>>
basic_record_reader<Types...>* unwrap(T* reader) {
    return static_cast<basic_record_reader<Types...>*>(reader);
}

inline record_reader* unwrap_record_reader(executor::record_reader* reader) {
    return unwrap<kind::int8, kind::float8>(reader);
}

}

