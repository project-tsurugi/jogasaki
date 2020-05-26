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

namespace jogasaki::executor::process::mock {

class external_writer : public executor::record_writer {
public:

    external_writer() = default;

    bool write(accessor::record_ref rec) override {
        return false;
    }
    void flush() override {
        // no-op
    }
    void release() override {
        // no-op
    }
};

}

