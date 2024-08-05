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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/io/record_channel_stats.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/status.h>

namespace jogasaki::api::impl {

class result_store_channel;

/**
 * @brief writer to write to result_store
 */
class result_store_channel_writer : public executor::io::record_writer {
public:
    /**
     * @brief create new object
     * @param parent parent result store channel
     * @param index the partition index (0-origin) that the writer belongs to.
     */
    result_store_channel_writer(
        result_store_channel& parent,
        std::size_t index
    ) noexcept;

    /**
     * @brief write the record
     * @param rec the record to write
     * @return true if flushed
     * @return false otherwise
     */
    bool write(accessor::record_ref rec) override;

    /**
     * @brief flush the writer
     */
    void flush() override;

    /**
     * @brief release the writer
     */
    void release() override;

    /**
     * @brief accessor to the partition index that the writer belongs
     * @return the partition index
     */
    [[nodiscard]] std::size_t index() const noexcept;

    /**
     * @brief write count
     */
    [[nodiscard]] std::size_t write_count() const noexcept;

private:
    result_store_channel* parent_{};
    std::size_t index_{};
};

/**
 * @brief the channel based on result_store
 */
class result_store_channel : public executor::io::record_channel {
public:
    /**
     * @brief create new object
     * @param store the base result store
     */
    explicit result_store_channel(
        maybe_shared_ptr<data::result_store> store
    ) noexcept;

    /**
     * @brief acquire writer
     * @param wrt [out] the writer to acquire
     * @return status::ok when successful
     * @return any other error
     */
    status acquire(std::shared_ptr<executor::io::record_writer>& wrt) override;

    /**
     * @brief accessor to the base result_store
     * @return the result_store that the channel is based on
     */
    data::result_store& store();

    /**
     * @brief metadata setter
     * @param m the metadata that the writer use
     * @return status::ok when successful
     * @return any other error
     */
    status meta(maybe_shared_ptr<meta::external_record_meta> m) override;

    /**
     * @brief accessor for channel stats
     */
    executor::io::record_channel_stats& statistics() override;

    /**
     * @brief accessor for record channel kind
     */
    [[nodiscard]] executor::io::record_channel_kind kind() const noexcept override{
        return executor::io::record_channel_kind::result_store_channel;
    }
private:
    maybe_shared_ptr<data::result_store> store_{};
    executor::io::record_channel_stats stats_{};
};

}
