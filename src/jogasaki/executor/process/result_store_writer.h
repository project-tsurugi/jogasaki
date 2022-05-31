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
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/data/iterable_record_store.h>

namespace jogasaki::executor::process {

using takatori::util::maybe_shared_ptr;

class cache_align result_store_writer : public record_writer {
public:
    ~result_store_writer() override = default;
    result_store_writer(result_store_writer const& other) = delete;
    result_store_writer& operator=(result_store_writer const& other) = delete;
    result_store_writer(result_store_writer&& other) noexcept = delete;
    result_store_writer& operator=(result_store_writer&& other) noexcept = delete;

    result_store_writer(
        data::iterable_record_store& store,
        maybe_shared_ptr<meta::record_meta> meta
    );

    bool write(accessor::record_ref rec) override;

    void flush() override;

    void release() override;

private:
    data::iterable_record_store* store_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}
