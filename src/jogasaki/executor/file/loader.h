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
    // parameter type
    meta::field_type_kind type_{};

    // 0-oring index in the parquet record
    std::size_t index_{};

    // value offset in the record read from parquet file
    std::size_t value_offset_{};

    // nullity offset in the record read from parquet file
    std::size_t nullity_offset_{};
};

/**
 * @brief result of load function
 */
enum class loader_result : std::int32_t {
    ok = 0,
    running,
    error,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(loader_result value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case loader_result::ok: return "ok"sv;
        case loader_result::running: return "running"sv;
        case loader_result::error: return "error"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, loader_result value) {
    return out << to_string_view(value);
}

/**
 * @brief loader to conduct reading files and executing statements
 */
class cache_align loader {
public:

    constexpr static std::size_t default_bulk_size = 10000;

    /**
     * @brief create empty object
     */
    loader() = default;

    ~loader() = default;
    loader(loader const& other) = delete;
    loader& operator=(loader const& other) = delete;
    loader(loader&& other) noexcept = delete;
    loader& operator=(loader&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    loader(
        std::vector<std::string> files,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::transaction* tx,
        std::size_t bulk_size = default_bulk_size
    ) noexcept;

    /**
     * @brief conduct part of the load requests
     * @return running there is more to do
     * @return ok if all load requests are done
     * @return error if any error occurs
     */
    loader_result operator()();

    /**
     * @brief accessor to the atomic counter for the currently executed statements
     * @return the counter
     */
    std::atomic_size_t& running_statement_count() noexcept;

    /**
     * @brief accessor to the total number of loaded records
     * @return the total number
     */
    [[nodiscard]] std::size_t records_loaded() const noexcept;

    /**
     * @brief accessor to the error information
     * @return the error status and message
     */
    [[nodiscard]] std::pair<status, std::string> error_info() const noexcept;

private:
    std::vector<std::string> files_{};
    std::atomic_size_t running_statement_count_{};
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    std::shared_ptr<parquet_reader> reader_{};
    api::impl::transaction* tx_{};
    std::atomic_size_t records_loaded_{0};
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    decltype(files_)::const_iterator next_file_{};
    std::unordered_map<std::string, parameter> mapping_{};
    std::size_t bulk_size_{};
    bool more_to_read_{true};
    status status_{status::ok};
    std::string msg_{};
    bool error_aborting_{false};
    bool error_aborted_{false};
};

}

}

