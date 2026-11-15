/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace tateyama::nsbench {

/**
 * @brief latency statistics in nanoseconds, same as sbench StatsEntity
 */
struct stats {
    std::int64_t count_{};  //NOLINT
    std::int64_t min_{};  //NOLINT
    std::int64_t max_{};  //NOLINT
    double average_{};  //NOLINT
    std::int64_t percentile50_{};  //NOLINT
    std::int64_t percentile95_{};  //NOLINT
};

/**
 * @brief calculate the statistics of the latencies
 * @param latencies the latencies in nanoseconds, sorted by this function
 */
stats get_stats(std::vector<std::int64_t>& latencies);

/**
 * @brief aggregate the statistics of the threads in the same way as sbench StatsUtil.getStatsOfStats
 * @details min/max are taken from all threads, while average and percentiles are those of the per-thread values
 */
stats get_stats_of_stats(std::vector<stats> const& list);

/**
 * @brief latency histogram with the same bins as sbench (<1ms, 1-3ms, ..., >300ms)
 */
class histogram {
public:
    histogram();

    void add(std::int64_t latency_ns);

    void add(histogram const& other);

    [[nodiscard]] std::vector<std::int64_t> const& counts() const noexcept {
        return counts_;
    }

    [[nodiscard]] std::int64_t total() const noexcept;

private:
    std::vector<std::int64_t> counts_{};
};

/**
 * @brief result of a benchmark measured by a thread or aggregated over threads
 */
struct result {
    std::string command_{};  //NOLINT
    std::string sub_command_{};  //NOLINT

    /// @brief number of threads, or empty for the result of a single thread
    std::optional<std::int64_t> threads_{};  //NOLINT
    std::int64_t elapsed_ms_{};  //NOLINT
    std::int64_t operations_per_tx_{};  //NOLINT
    stats tx_{};  //NOLINT

    /// @brief latencies before commit. Empty for noptx.
    std::optional<stats> bc_{};  //NOLINT
    std::string transaction_{};  //NOLINT
    histogram histogram_{};  //NOLINT
};

/**
 * @brief print the results in the same format as sbench
 * @details results are printed to stdout and appended to the csv file if the output path is given
 */
class reporter {
public:
    explicit reporter(bool header) noexcept :
        header_(header)
    {}

    /**
     * @brief print the result
     * @param res the result to print
     * @param output_path the csv file name, or empty not to write the file
     * @param always_print_header whether to print the header to stdout even if it's printed already
     */
    void report(result const& res, std::string const& output_path, bool always_print_header);

private:
    bool header_{};
    bool printed_header_{};
    bool printed_noptx_header_{};
    bool printed_histogram_header_{};

    void write_file(std::string const& path, std::string const& header, std::string const& line) const;
    void report_histogram(histogram const& h, std::string const& output_path);
};

/**
 * @brief format the integer with the thousands separator same as java "%,d"
 */
std::string format_grouped(std::int64_t value);

/**
 * @brief replace all occurrences of `from` with `to`, same as java String.replace
 */
std::string replace_all(std::string_view src, std::string_view from, std::string_view to);

}  // namespace tateyama::nsbench
