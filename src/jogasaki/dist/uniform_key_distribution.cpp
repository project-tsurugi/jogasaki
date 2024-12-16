/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include "uniform_key_distribution.h"

#include <glog/logging.h>

#include <jogasaki/kvs/iterator.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/modify_status.h>

namespace jogasaki::dist {

std::optional<double> uniform_key_distribution::estimate_count(range_type const&) {
    return std::nullopt;
}

std::optional<double> uniform_key_distribution::estimate_key_size(range_type const&) {
    return std::nullopt;
}

std::optional<double> uniform_key_distribution::estimate_value_size(range_type const&) {
    return std::nullopt;
}

status read_key_entry(
    std::unique_ptr<kvs::iterator>& it,
    kvs::transaction& tx,
    std::string& out
) {
    std::string_view k{};
    if (auto r = it->read_key(k); r != status::ok) {
        utils::modify_concurrent_operation_status(tx, r, true);
        if(r == status::not_found) {
            return r;
        }
        (void) tx.abort();
        return r;
    }
    out = std::string{k};
    return status::ok;
}

status uniform_key_distribution::scan_one(bool reverse, uniform_key_distribution::pivot_type& out) {
    std::unique_ptr<kvs::iterator> it{};
    if(auto res = stg_->content_scan(
            *tx_,
            "",
            kvs::end_point_kind::unbound,
            "",
            kvs::end_point_kind::unbound,
            it,
            1, // fetch only one entry
            reverse
        );
        res != status::ok || !it) {
            return res;
    }
    if(auto res = it->next(); res != status::ok) {
        return res;
    }
    if(auto res = read_key_entry(it, *tx_, out); res != status::ok) {
        return res;
    }
    return status::ok;
}

status uniform_key_distribution::lowkey(pivot_type& out) {
    return scan_one(false, out);
}

status uniform_key_distribution::highkey(pivot_type& out) {
    return scan_one(true, out);
}

double string_distance(std::string_view lo, std::string_view hi) {
    std::size_t len = std::max(lo.size(), hi.size());
    double dist{};

    double div = 1.0;
    for(std::size_t i=0; i < len; ++i) {
        int l = i < lo.size() ? lo[i] : 0;
        int h = i < hi.size() ? hi[i] : 0;
        dist += (h - l) * div;
        div /= 256.0;
    }
    return dist;
}

std::size_t common_prefix_len(std::string_view lo, std::string_view hi) {
    std::size_t len = std::min(lo.size(), hi.size());
    std::size_t i = 0;
    while(i < len) {
        if(lo[i] != hi[i]) {
            break;
        }
        ++i;
    }
    return i;
}

std::vector<std::string> generate_strings(std::string_view lo, std::string_view hi, std::size_t chars) {
    auto cpl = common_prefix_len(lo, hi);

    char h = cpl < hi.size() ? hi[cpl] : 0;
    char l = cpl < lo.size() ? lo[cpl] : 0;
    std::size_t cnt = h - l;

    std::vector<std::string> pivots{};
    pivots.reserve(cnt * chars);

    for (std::size_t j = 0; j < cnt; ++j) {
        std::string prefix{lo.data(), cpl};
        prefix += static_cast<char>(l + j);

        for (std::size_t i = 0; i < chars; ++i) {
            pivots.emplace_back(prefix + static_cast<char>(i));
        }
    }
    return pivots;
}

std::vector<uniform_key_distribution::pivot_type> uniform_key_distribution::compute_pivots(
    size_type max_count,
    range_type const& range
) {
    std::string high{};
    std::string low{};
    if(auto res = lowkey(low); res != status::ok) {
        // empty index or failing to get low key somehow
        return {};
    }
    if(range.begin_endpoint() != kvs::end_point_kind::unbound && range.begin_key() > low) {
        low = range.begin_key();
    }
    if(auto res = highkey(high); res != status::ok) {
        // empty index or failing to get high key somehow
        return {};
    }
    if(range.end_endpoint() != kvs::end_point_kind::unbound && range.end_key() < high) {
        high = range.end_key();
    }

    std::vector<pivot_type> pivots = generate_strings(low, high);

    if (max_count < pivots.size()) {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(pivots.begin(), pivots.end(), g);
        pivots.resize(max_count);
    }

    std::sort(pivots.begin(), pivots.end());

    if(VLOG_IS_ON(log_debug)) {
        std::stringstream ss{};
        ss << "pivot_count:" << pivots.size();
        ss << " pivots:[";
        bool first = true;
        for(auto&& p : pivots) {
            if(! first) {
                ss << ",";
            }
            ss << "\"";
            ss << utils::binary_printer{p.data(), p.size()};
            ss << "\"";
            first = false;
        }
        ss << "]";
        VLOG_LP(log_debug) << ss.str();
    }
    return pivots;
}

} // namespace jogasaki::dist
