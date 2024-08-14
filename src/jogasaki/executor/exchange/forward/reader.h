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

#include <jogasaki/executor/io/record_reader.h>
#include <jogasaki/utils/interference_size.h>

#include "forward_info.h"
#include "input_partition.h"

namespace jogasaki::executor::exchange::forward {

class cache_align reader : public io::record_reader {
public:
    reader() = default;
    ~reader() override = default;

    reader(reader const& other) = delete;
    reader& operator=(reader const& other) = delete;
    reader(reader&& other) noexcept = delete;
    reader& operator=(reader&& other) noexcept = delete;

    reader(
        std::shared_ptr<forward_info> info,
        std::shared_ptr<input_partition> const& partition,
        std::shared_ptr<std::atomic_bool> sink_active
    );

    [[nodiscard]] bool available() const override {
        return partition() && ! partition()->empty();
    }

    [[nodiscard]] bool next_record() override {
        if(! partition()) {
            return false;
        }
        return partition()->try_pop(current_record_);
    }

    [[nodiscard]] accessor::record_ref get_record() const override {
        return current_record_;
    }

    void release() override {
        // no-op
    }

    [[nodiscard]] bool active() const noexcept {
        return sink_active_->load();
    }

    [[nodiscard]] std::shared_ptr<input_partition> const& partition() const noexcept {
        return *partition_ptr_;
    }

    [[nodiscard]] std::shared_ptr<forward_info> const& info() const noexcept {
        return info_;
    }

private:
    std::shared_ptr<forward_info> info_{};
    std::shared_ptr<input_partition> const* partition_ptr_{};
    std::shared_ptr<std::atomic_bool> sink_active_{};
    accessor::record_ref current_record_{};
};

}  // namespace jogasaki::executor::exchange::forward
