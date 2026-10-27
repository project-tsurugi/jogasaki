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
#include "report.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <numeric>
#include <sstream>

namespace tateyama::nsbench {

namespace {

// histogram bin boundaries in microseconds, same as sbench Scenario.BIN_BOUNDARIES
constexpr std::array<std::int64_t, 9> bin_boundaries{1000, 3000, 5000, 10000, 20000, 40000, 80000, 150000, 300000};

// same as java Math.round
std::int64_t round_half_up(double v) {
    return static_cast<std::int64_t>(std::floor(v + 0.5));
}

// nanoseconds to rounded microseconds
std::int64_t us(double ns) {
    return round_half_up(ns / 1000);
}

// same as sbench StatsUtil.percentile (nearest rank)
std::int64_t percentile(std::vector<std::int64_t> const& sorted, double p) {
    if (sorted.empty()) {
        return 0;
    }
    auto index = static_cast<std::size_t>(std::ceil(p / 100.0 * static_cast<double>(sorted.size())));
    return sorted[std::max(index, static_cast<std::size_t>(1)) - 1];
}

std::string quoted(std::int64_t v) {
    return "\"" + format_grouped(v) + "\"";
}

}  // namespace

stats get_stats(std::vector<std::int64_t>& latencies) {
    stats ret{};
    if (latencies.empty()) {
        return ret;
    }
    std::sort(latencies.begin(), latencies.end());
    ret.count_ = static_cast<std::int64_t>(latencies.size());
    ret.min_ = latencies.front();
    ret.max_ = latencies.back();
    ret.average_ = std::accumulate(latencies.begin(), latencies.end(), 0.0) / static_cast<double>(latencies.size());
    ret.percentile50_ = percentile(latencies, 50);
    ret.percentile95_ = percentile(latencies, 95);
    return ret;
}

stats get_stats_of_stats(std::vector<stats> const& list) {
    stats ret{};
    if (list.empty()) {
        return ret;
    }
    std::vector<std::int64_t> p50{};
    std::vector<std::int64_t> p95{};
    ret.min_ = list.front().min_;
    ret.max_ = list.front().max_;
    double sum_average{};
    for(auto&& s : list) {
        ret.count_ += s.count_;
        ret.min_ = std::min(ret.min_, s.min_);
        ret.max_ = std::max(ret.max_, s.max_);
        sum_average += s.average_;
        p50.emplace_back(s.percentile50_);
        p95.emplace_back(s.percentile95_);
    }
    ret.average_ = sum_average / static_cast<double>(list.size());
    std::sort(p50.begin(), p50.end());
    std::sort(p95.begin(), p95.end());
    ret.percentile50_ = percentile(p50, 50);
    ret.percentile95_ = percentile(p95, 95);
    return ret;
}

histogram::histogram() :
    counts_(bin_boundaries.size() + 1)
{}

void histogram::add(std::int64_t latency_ns) {
    auto latency_us = static_cast<double>(latency_ns) / 1000;
    // bin i covers [boundary[i-1], boundary[i])
    std::size_t i = 0;
    while(i < bin_boundaries.size() && latency_us >= static_cast<double>(bin_boundaries[i])) {
        ++i;
    }
    ++counts_[i];
}

void histogram::add(histogram const& other) {
    for(std::size_t i = 0; i < counts_.size(); ++i) {
        counts_[i] += other.counts_[i];
    }
}

std::int64_t histogram::total() const noexcept {
    return std::accumulate(counts_.begin(), counts_.end(), static_cast<std::int64_t>(0));
}

std::string format_grouped(std::int64_t value) {
    auto digits = std::to_string(value < 0 ? -value : value);
    std::string ret{};
    for(std::size_t i = 0; i < digits.size(); ++i) {
        if (i != 0 && (digits.size() - i) % 3 == 0) {
            ret += ',';
        }
        ret += digits[i];
    }
    return value < 0 ? "-" + ret : ret;
}

std::string replace_all(std::string_view src, std::string_view from, std::string_view to) {
    std::string ret{};
    std::size_t pos = 0;
    while(true) {
        auto found = src.find(from, pos);
        if (found == std::string_view::npos || from.empty()) {
            ret += src.substr(pos);
            break;
        }
        ret += src.substr(pos, found - pos);
        ret += to;
        pos = found + from.size();
    }
    return ret;
}

void reporter::write_file(std::string const& path, std::string const& header, std::string const& line) const {
    if (path.empty()) {
        return;
    }
    std::ofstream out{path, std::ios::app};
    if (! out) {
        std::cerr << "failed to open output file: " << path << std::endl;
        return;
    }
    if (header_) {
        out << header << '\n';
    }
    out << line << '\n';
}

void reporter::report(result const& res, std::string const& output_path, bool always_print_header) {
    std::ostringstream header{};
    std::ostringstream line{};
    auto const& tx = res.tx_;
    auto elapsed_s = std::max(static_cast<double>(res.elapsed_ms_) / 1000.0, 1.0);
    bool* printed{};
    if (res.bc_) {
        auto const& bc = *res.bc_;
        auto operations = tx.count_ * res.operations_per_tx_;
        auto ops = round_half_up(static_cast<double>(operations) / elapsed_s);
        auto tps = res.operations_per_tx_ == 0 ? 0 : ops / res.operations_per_tx_;
        header << "Benchmark,Sub,Elapsed(ms)," << (res.threads_ ? "Threads," : "")
               << "Operations,OPS,Ope/Tx,TPS,Avg-Tx(μs),Avg-Bc(μs),Min-Tx(μs),Min-Bc(μs),Max-Tx(μs),Max-Bc(μs),"
                  "50Pctl-Tx(μs),50Pctl-Bc(μs),95Pctl-Tx(μs),95Pctl-Bc(μs),Tx";
        line << res.command_ << "," << res.sub_command_ << "," << quoted(res.elapsed_ms_) << ",";
        if (res.threads_) {
            line << quoted(*res.threads_) << ",";
        }
        line << quoted(operations) << "," << quoted(ops) << "," << quoted(res.operations_per_tx_) << "," << quoted(tps) << ","
             << quoted(us(tx.average_)) << "," << quoted(us(bc.average_)) << ","
             << quoted(us(static_cast<double>(tx.min_))) << "," << quoted(us(static_cast<double>(bc.min_))) << ","
             << quoted(us(static_cast<double>(tx.max_))) << "," << quoted(us(static_cast<double>(bc.max_))) << ","
             << quoted(us(static_cast<double>(tx.percentile50_))) << "," << quoted(us(static_cast<double>(bc.percentile50_))) << ","
             << quoted(us(static_cast<double>(tx.percentile95_))) << "," << quoted(us(static_cast<double>(bc.percentile95_))) << ","
             << res.transaction_;
        printed = &printed_header_;
    } else {
        auto operations = tx.count_;
        auto tps = round_half_up(static_cast<double>(operations) / elapsed_s);
        header << "Benchmark,Elapsed(ms)," << (res.threads_ ? "Threads," : "")
               << "Operations,TPS,Avg-Tx(μs),Min-Tx(μs),Max-Tx(μs),50Pctl-Tx(μs),95Pctl-Tx(μs),Tx";
        line << res.command_ << "," << quoted(res.elapsed_ms_) << ",";
        if (res.threads_) {
            line << quoted(*res.threads_) << ",";
        }
        line << quoted(operations) << "," << quoted(tps) << ","
             << quoted(us(tx.average_)) << ","
             << quoted(us(static_cast<double>(tx.min_))) << ","
             << quoted(us(static_cast<double>(tx.max_))) << ","
             << quoted(us(static_cast<double>(tx.percentile50_))) << ","
             << quoted(us(static_cast<double>(tx.percentile95_))) << ","
             << res.transaction_;
        printed = &printed_noptx_header_;
    }
    if (always_print_header || ! *printed) {
        std::cout << header.str() << '\n';
        *printed = true;
    }
    std::cout << line.str() << std::endl;
    write_file(output_path, header.str(), line.str());
    report_histogram(res.histogram_, output_path);
}

void reporter::report_histogram(histogram const& h, std::string const& output_path) {
    std::ostringstream header{};
    std::ostringstream line{};
    auto n = bin_boundaries.size();
    header << "<" << bin_boundaries[0] / 1000 << "ms,";
    for(std::size_t i = 0; i + 1 < n; ++i) {
        header << bin_boundaries[i] / 1000 << "-" << bin_boundaries[i + 1] / 1000 << "ms,";
    }
    header << ">" << bin_boundaries[n - 1] / 1000 << "ms,total,";
    for(std::size_t i = 0; i < n; ++i) {
        header << "-" << bin_boundaries[i] / 1000 << "ms(%),";
    }
    header << bin_boundaries[n - 1] / 1000 << "ms-(%),";

    auto const& counts = h.counts();
    auto total = h.total();
    for(auto c : counts) {
        line << c << ",";
    }
    line << total << ",";
    std::int64_t stacked{};
    for(std::size_t i = 0; i < counts.size(); ++i) {
        stacked += counts[i];
        if (total == 0) {
            line << "NaN";
        } else {
            std::array<char, 32> buf{};
            std::snprintf(buf.data(), buf.size(), "%.1f", static_cast<double>(stacked) / static_cast<double>(total) * 100);  //NOLINT
            line << buf.data();
        }
        if (i < n) {
            line << ",";
        }
    }
    if (! printed_histogram_header_) {
        std::cout << header.str() << '\n';
        printed_histogram_header_ = true;
    }
    std::cout << line.str() << std::endl;
    if (! output_path.empty()) {
        write_file(replace_all(output_path, ".csv", "_hist.csv"), header.str(), line.str());
    }
}

}  // namespace tateyama::nsbench
