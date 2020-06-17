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

#include <jogasaki/constants.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/executor/record_writer.h>
#include "input_partition.h"
#include "shuffle_info.h"
#include "source.h"
#include "sink.h"

namespace jogasaki::executor::exchange::aggregate {

class writer : public record_writer {
public:
    ~writer() override = default;
    writer(writer const& other) = delete;
    writer& operator=(writer const& other) = delete;
    writer(writer&& other) noexcept = delete;
    writer& operator=(writer&& other) noexcept = delete;
    writer(std::size_t downstream_partitions, std::shared_ptr<shuffle_info> info, std::vector<std::unique_ptr<input_partition>>& partitions, aggregate::sink& owner) :
            downstream_partitions_(downstream_partitions),
            partitions_(partitions),
            info_(std::move(info)),
            partitioner_(downstream_partitions_, info_->key_meta()),
            owner_(std::addressof(owner))
    {}

    bool write(accessor::record_ref rec) override {
        auto partition = partitioner_(info_->extract_key(rec));
        initialize_lazy(partition);
        partitions_[partition]->write(rec);
        return false;
    }

    void flush() override {
        for(std::size_t i=0; i < downstream_partitions_; ++i) {
            if (partitions_[i]) {
                partitions_[i]->flush();
            }
        }
    }

    void release() override {
        owner_->release_writer(*this);
    }

private:
    std::size_t downstream_partitions_{default_partitions};
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<shuffle_info> info_{};
    partitioner partitioner_{};
    sink* owner_{};

    void initialize_lazy(std::size_t partition) {
        if (partitions_.empty()) {
            partitions_.resize(downstream_partitions_);
        }
        if (partitions_[partition]) return;
        partitions_[partition] = std::make_unique<input_partition>(
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool()),
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool()),
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool()),
            std::make_unique<memory::monotonic_paged_memory_resource>(&global::page_pool()),
            info_,
            owner_->context()
        );
    }
};

}
