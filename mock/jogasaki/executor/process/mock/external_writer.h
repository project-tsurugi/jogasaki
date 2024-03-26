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

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::mock {

using takatori::util::maybe_shared_ptr;
using kind = meta::field_type_kind;

class cache_align basic_external_writer : public io::record_writer {
public:
    using record_type = jogasaki::mock::basic_record;
    using records_type = std::vector<record_type>;

    /**
     * @brief create empty instance
     */
    basic_external_writer() = default;

    /**
     * @brief create new instance considering field metadata and its mapping
     * @param meta metadata of the record_ref passed to write()
     */
    explicit basic_external_writer(maybe_shared_ptr<meta::record_meta> meta) :
        meta_(std::move(meta))
    {}

    /**
     * @brief write record and store internal storage as basic_record.
     */
    bool write(accessor::record_ref rec) override {
        record_type r{rec, maybe_shared_ptr<meta::record_meta>{meta_.get()}, resource_.get()};
        records_.emplace_back(r);
        return true;
    }

    void flush() override {
        // no-op
    }

    void release() override {
        released_ = true;
    }

    void acquire() {
        acquired_ = true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return records_.size();
    }

    [[nodiscard]] records_type const& records() const noexcept {
        return records_;
    }
    [[nodiscard]] bool is_released() const noexcept {
        return released_;
    }
    [[nodiscard]] bool is_acquired() const noexcept {
        return acquired_;
    }
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    records_type records_{};
    bool released_{false};
    bool acquired_{false};
    std::unique_ptr<memory::paged_memory_resource> resource_{
        std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())
    };
};

}

