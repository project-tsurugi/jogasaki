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
#pragma once

#include <jogasaki/executor/record_writer.h>

namespace jogasaki::executor::exchange::broadcast {

class sink : public record_writer {
public:
    ~sink() override = default;
    sink(sink&& other) noexcept = delete;
    sink& operator=(sink&& other) noexcept = delete;
    sink() {}
    bool write(accessor::record_ref) override {
        return false;
    }
    void flush() override {}

    /**
     * @brief sources setter
     * @param sources
     */
    void target_source(source& source) {
        source_ = &source;
    }
private:
    source* source_{};
};

}
