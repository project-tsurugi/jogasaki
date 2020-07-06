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

#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>

namespace jogasaki::executor::process::impl::relop {

enum class relop_kind : std::size_t {
    emitter,
    scanner,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
constexpr inline std::string_view to_string_view(relop_kind value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case relop_kind::emitter: return "emitter"sv;
        case relop_kind::scanner: return "scanner"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, relop_kind value) {
    return out << to_string_view(value);
}

/// @brief a set of expression_kind.
using relop_kind_set = takatori::util::enum_set<
    relop_kind,
    relop_kind::emitter,
    relop_kind::scanner>;

/**
 * @brief relational operator base class
 */
class base {
public:
    /**
     * @brief create empty object
     */
    base() = default;

    /**
     * @brief create new object
     */
    base(
            std::shared_ptr<meta::record_meta> meta,
            std::shared_ptr<data::record_store> store) :
            meta_(std::move(meta)),
            store_(std::move(store)) {
    }

    virtual ~base() = default;

    virtual relop_kind kind() = 0;

private:
    std::shared_ptr<meta::record_meta> meta_{};
    std::shared_ptr<data::record_store> store_{};
};

}


