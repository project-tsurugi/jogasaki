/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <takatori/util/exception.h>

#include <jogasaki/kvs/iterator.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/plan/plan_exception.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/modify_status.h>

namespace jogasaki::dist {

using takatori::util::throw_exception;

std::optional<double> uniform_key_distribution::estimate_count(range_type const&) {
    return std::nullopt;
}

std::optional<double> uniform_key_distribution::estimate_key_size(range_type const&) {
    return std::nullopt;
}

std::optional<double> uniform_key_distribution::estimate_value_size(range_type const&) {
    return std::nullopt;
}

static status read_key_entry(
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
        (void) tx.abort_transaction();
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
        ); res != status::ok) {
        if(req_ctx_) {
            handle_kvs_errors(*req_ctx_, res);
            handle_generic_error(*req_ctx_, res, error_code::sql_execution_exception);
        }
        return res;
    }
    if(auto res = it->next(); res != status::ok) {
        if(res != status::not_found && req_ctx_) {
            handle_kvs_errors(*req_ctx_, res);
            handle_generic_error(*req_ctx_, res, error_code::sql_execution_exception);
        }
        return res;
    }
    if(auto res = read_key_entry(it, *tx_, out); res != status::ok) {
        if(res != status::not_found && req_ctx_) {
            handle_kvs_errors(*req_ctx_, res);
            handle_generic_error(*req_ctx_, res, error_code::sql_execution_exception);
        }
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

std::vector<std::string> generate_strings2(std::uint64_t max_count, std::string_view lo, std::string_view hi) {
    // divide next 4-octets (32 bit) after common_prefix

    if (hi <= lo) {
        // invalid arguments or one point
        return {};
    }

    auto head_32bit = [](std::string_view sv) {
        std::uint64_t ret = 0U;
        for (unsigned int i = 0; i < 4; i++) {
            unsigned char c = (i >= sv.size()) ? 0U : static_cast<unsigned char>(sv[i]);
            ret = (ret << 8U) | c;
        }
        return ret;
    };

    auto cpl = common_prefix_len(lo, hi);
    std::uint64_t h = head_32bit(hi.substr(cpl));  // round down
    std::uint64_t l = head_32bit(lo.substr(cpl)) + ((lo.size() <= cpl + 4) ? 0 : 1);  // round up
    // h-l < 1UL<<32
    std::uint64_t count = std::min(max_count, (1UL << 24U) - 1U);

    std::vector<std::string> pivots{};
    pivots.reserve(count);

    std::string buf{lo.substr(0, cpl + 4)};
    buf.resize(cpl + 4);
    std::uint64_t prev_c32 = 0U;
    for (std::size_t i = 0; i < count; i++) {
        // weighted mean : l < c32 < h <= UINT32_MAX
        std::uint64_t c32 = l + (h - l) * (i + 1) / (count + 1);
        // (h - l) * (i + 1) < 1UL<<56 ; so never overflow uint64
        buf[cpl + 0] = static_cast<char>(c32 >> 24U);
        buf[cpl + 1] = static_cast<char>(c32 >> 16U);
        buf[cpl + 2] = static_cast<char>(c32 >> 8U);
        buf[cpl + 3] = static_cast<char>(c32);
        bool skip = buf <= lo || hi <= buf || (i > 0 && c32 <= prev_c32);
        prev_c32 = c32;
        if (skip) {
            continue;
        }
        pivots.emplace_back(buf);
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
        if(res == status::not_found) {
            // empty index or failing to get low key somehow
            return {};
        }
        // other unrecoverable errors - tx possibly aborted and scan cannot continue
        throw_exception(jogasaki::plan::plan_exception{req_ctx_->error_info()});
    }
    if(range.begin_endpoint() != kvs::end_point_kind::unbound && range.begin_key() > low) {
        low = range.begin_key();
    }
    if(auto res = highkey(high); res != status::ok) {
        if(res == status::not_found) {
            // empty index or failing to get high key somehow
            return {};
        }
        // other unrecoverable errors - tx possibly aborted and scan cannot continue
        throw_exception(jogasaki::plan::plan_exception{req_ctx_->error_info()});
    }
    if(range.end_endpoint() != kvs::end_point_kind::unbound && range.end_key() < high) {
        high = range.end_key();
    }

    std::vector<pivot_type> pivots = generate_strings2(max_count, low, high);

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
        LOG_LP(INFO) << ss.str();
    }
    return pivots;
}

} // namespace jogasaki::dist
