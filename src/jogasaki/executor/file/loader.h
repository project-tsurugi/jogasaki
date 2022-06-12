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

#include <atomic>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/executor/file/parquet_reader.h>

namespace jogasaki {
class request_context;

namespace api::impl {
class transaction;
}

namespace executor::file {

using takatori::util::maybe_shared_ptr;

struct parameter {
    meta::field_type_kind type_{};
    std::size_t index_{};
    std::size_t value_offset_{};
    std::size_t nullity_offset_{};
};

/**
 * @brief context object for the job
 * @details this class represents context information in the scope of the job scheduling
 */
class cache_align loader {
public:

    constexpr static std::size_t default_bulk_size = 10000;

    /**
     * @brief create default context object
     */
    loader() = default;

    ~loader() = default;
    loader(loader const& other) = delete;
    loader& operator=(loader const& other) = delete;
    loader(loader&& other) noexcept = delete;
    loader& operator=(loader&& other) noexcept = delete;

    loader(
        std::vector<std::string> files,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::database* db,
        api::impl::transaction* tx,
        std::size_t bulk_size = default_bulk_size
    ) noexcept;


    /**
     * @brief conduct part of the load requests
     * @return true if there is more to do
     * @return false if all load requests are done
     */
    bool operator()();

    std::atomic_size_t& run_count() noexcept {
        return running_statements_;
    }

    [[nodiscard]] std::size_t loaded_record_count() const noexcept {
        return count_.load();
    }

private:
    std::vector<std::string> files_{};
    std::atomic_size_t running_statements_{};
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    std::shared_ptr<parquet_reader> reader_{};
    api::database* db_{};
    api::impl::transaction* tx_{};
    std::atomic_size_t count_{0};
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    decltype(files_)::const_iterator next_file_{};
    std::unordered_map<std::string, parameter> mapping_{};
    std::size_t bulk_size_{};
    bool more_to_read_{true};
};

}

}

