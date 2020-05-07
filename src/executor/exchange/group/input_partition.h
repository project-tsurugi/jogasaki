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

#include <boost/container/pmr/vector.hpp>
#include <takatori/util/universal_extractor.h>
#include <takatori/util/reference_list_view.h>

#include <accessor/record_ref.h>
#include <data/variable_legnth_data_region.h>
#include <executor/global.h>
#include <executor/record_writer.h>
#include <executor/exchange/group/shuffle_info.h>
#include <memory/page_pool.h>

namespace jogasaki::executor::exchange::group {

/**
 * @brief partitioned input data processed in upper phase in shuffle
 */
class input_partition {
public:
    using pointer_table_type = boost::container::pmr::vector<void*>;
    using iterator = pointer_table_type::iterator;

    input_partition() = default;
    ~input_partition() = default;
    input_partition(input_partition&& other) noexcept = delete;
    input_partition& operator=(input_partition&& other) noexcept = delete;

    input_partition(std::unique_ptr<memory::paged_memory_resource> resource, std::shared_ptr<shuffle_info> info) :
            resource_(std::move(resource)),
            info_(std::move(info)),
            pointer_table_(boost::container::pmr::polymorphic_allocator<void*>(resource_.get())),
            comparator_(info_->key_meta())
    {}

    bool write(accessor::record_ref r) {
        initialize_lazy();
        pointer_table_.emplace_back(records_->append(r.data(), info_->record_meta()->record_size()));
        if (pointer_table_.capacity() == pointer_table_.size()) {
            flush();
            return true;
        }
        return false;
    }

    void flush() {
        auto sz = info_->record_meta()->record_size();
        std::sort(pointer_table_.begin(), pointer_table_.end(), [&](auto const&x, auto const& y){
            return comparator_(info_->extract_key(accessor::record_ref(x, sz)),
                    info_->extract_key(accessor::record_ref(y, sz))) < 0;
        });
    }

    iterator begin() {
        return pointer_table_.begin();
    }

    iterator end() {
        return pointer_table_.end();
    }

private:
    std::unique_ptr<memory::paged_memory_resource> resource_{};
    std::shared_ptr<shuffle_info> info_;
    std::unique_ptr<data::variable_length_data_region> records_{};
    boost::container::pmr::vector<void*> pointer_table_{};
    comparator comparator_{};

    void initialize_lazy() {
        if (records_) return;
        pointer_table_.reserve(memory::page_size/sizeof(void*));
        records_ = std::make_unique<data::variable_length_data_region>(resource_.get(), info_->record_meta()->record_alignment());
    }
};

}
