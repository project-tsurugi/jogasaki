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

#include <iomanip>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <takatori/util/enum_set.h>

#include "commit_response.h"

namespace jogasaki {

enum class request_cancel_kind : std::int32_t {
    undefined = 0,
    write,
    scan,
    find,
    group,
    take_cogroup,
    take_group,
    take_flat,
    transaction_begin_wait,
    transaction_precommit,
    transaction_durable_wait,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(request_cancel_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = request_cancel_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::write: return "write"sv;
        case kind::scan: return "scan"sv;
        case kind::find: return "find"sv;
        case kind::group: return "group"sv;
        case kind::take_cogroup: return "take_cogroup"sv;
        case kind::take_group: return "take_group"sv;
        case kind::take_flat: return "take_flat"sv;
        case kind::transaction_begin_wait: return "transaction_begin_wait"sv;
        case kind::transaction_precommit: return "transaction_precommit"sv;
        case kind::transaction_durable_wait: return "transaction_durable_wait"sv;
    }
    std::abort();
}

/**
 * @brief configuration to enable request cancellation
 */
class request_cancel_config {
public:
    static constexpr std::size_t numa_node_unspecified = static_cast<std::size_t>(-1);

    [[nodiscard]] bool is_enabled(request_cancel_kind value) const noexcept {
        return enabled_kinds_.contains(value);
    }

    void enable(request_cancel_kind arg) {
        enabled_kinds_.insert(arg);
    }

    friend inline std::ostream& operator<<(std::ostream& out, request_cancel_config const& cfg) {
        bool first = true;
        for(auto&& v: cfg.enabled_kinds_) {
            if(! first) {
                out << ",";
            }
            first = false;
            out << to_string_view(v);
        }
        return out;
    }

private:
    takatori::util::
        enum_set<request_cancel_kind, request_cancel_kind::write, request_cancel_kind::transaction_durable_wait>
            enabled_kinds_;
};

}  // namespace jogasaki
