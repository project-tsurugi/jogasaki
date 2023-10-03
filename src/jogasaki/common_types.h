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

#include <sharksfin/api.h>

#include <yugawara/storage/sequence.h>

namespace jogasaki {

using sequence_definition_id = ::yugawara::storage::sequence::definition_id_type;

using sequence_id = ::sharksfin::SequenceId;

using sequence_value = ::sharksfin::SequenceValue;

using sequence_version = ::sharksfin::SequenceVersion;

constexpr sequence_version version_invalid = static_cast<sequence_version>(-1);

/**
 * @brief sequence version value pair
 */
struct sequence_versioned_value {
    sequence_version version_;
    sequence_value value_;
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(sequence_versioned_value const& a, sequence_versioned_value const& b) noexcept {
    return a.version_ == b.version_ &&
        a.value_ == b.value_;
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(sequence_versioned_value const& a, sequence_versioned_value const& b) noexcept {
    return !(a == b);
}
}

