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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/data/iterable_record_store.h>

namespace jogasaki::executor::process {

using takatori::util::maybe_shared_ptr;

class cache_align external_writer : public record_writer {
public:
    ~external_writer() override = default;
    external_writer(external_writer const& other) = delete;
    external_writer& operator=(external_writer const& other) = delete;
    external_writer(external_writer&& other) noexcept = delete;
    external_writer& operator=(external_writer&& other) noexcept = delete;

    external_writer(
        data::iterable_record_store& store,
        maybe_shared_ptr<meta::record_meta> meta
    ) :
        store_(std::addressof(store)),
        meta_(std::move(meta))
    {}

    bool write(accessor::record_ref rec) override {
        store_->append(rec);
        return false;
    }

    void flush() override {
        // no-op
    }

    void release() override {
        store_ = nullptr;
    }

private:
    data::iterable_record_store* store_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}
