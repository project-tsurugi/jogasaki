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

namespace jogasaki::executor::process::mock {

using namespace testing;

class record_reader : public executor::record_reader {
public:
    static constexpr std::size_t record_count = 3;
    [[nodiscard]] bool available() const override {
        return true;
    }

    bool next_record() override {
        record_ = record(count_++, count_*100);
        return count_ <= record_count;
    }

    [[nodiscard]] accessor::record_ref get_record() const {
        return {static_cast<void*>(const_cast<record*>(&record_)), sizeof(record)};
    }

    void release() override {
        released_ = true;
    }

    record record_{};
    std::size_t count_{};
    bool released_{false};
};

}

