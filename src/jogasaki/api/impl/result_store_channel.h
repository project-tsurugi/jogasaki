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

#include <jogasaki/executor/record_channel.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::api::impl {

class result_store_channel;

class result_store_channel_writer : public executor::record_writer {
public:
    result_store_channel_writer(
        result_store_channel& parent,
        std::size_t index
    ) noexcept;

    bool write(accessor::record_ref rec) override;

    void flush() override {

    }

    void release() override {

    }

    std::size_t index() const noexcept;

private:
    result_store_channel* parent_{};
    std::size_t index_{};
};

class result_store_channel : public executor::record_channel {
public:
    explicit result_store_channel(
        maybe_shared_ptr<data::result_store> store
    ) noexcept;

    status acquire(std::shared_ptr<executor::record_writer>& wrt) override;

    status release(executor::record_writer& wrt) override;

    data::result_store& store() {
        return *store_;
    }
private:
    maybe_shared_ptr<data::result_store> store_{};
    impl::record_meta meta_{};
};

}
