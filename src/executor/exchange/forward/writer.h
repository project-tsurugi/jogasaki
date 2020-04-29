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

#include <constants.h>
#include <executor/global.h>
#include <memory/monotonic_paged_memory_resource.h>
#include <executor/record_writer.h>
#include "source.h"
#include "sink.h"

namespace jogasaki::executor::exchange::forward {

class writer : public record_writer {
public:
    writer(writer&& other) noexcept = delete;
    writer& operator=(writer&& other) noexcept = delete;
    writer() = default;

    bool write(accessor::record_ref rec) override {
        (void)rec;
        return false;
    }

    void flush() override {
    }

    void release() override {

    }

private:
};

}
