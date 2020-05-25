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

#include <memory>

#include <jogasaki/executor/record_reader.h>
#include <jogasaki/executor/group_reader.h>

namespace jogasaki::executor {

/**
 * @brief reader kind
 */
enum class reader_kind {
    record,
    group,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
constexpr inline std::string_view to_string_view(reader_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = reader_kind;
    switch (value) {
        case kind::record: return "record"sv;
        case kind::group: return "group"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, reader_kind value) {
    return out << to_string_view(value);
}

template <class T>
inline constexpr reader_kind to_kind = reader_kind::record;

template <>
inline constexpr reader_kind to_kind<group_reader> = reader_kind::group;

/**
 * @brief reader container to accommodate both record/group readers by type erasure
 */
class reader_container {
public:
    /**
     * @brief create empty instance
     */
    constexpr reader_container() = default;
    explicit reader_container(record_reader* reader) noexcept : reader_(reader), kind_(reader_kind::record) {}
    explicit reader_container(group_reader* reader) noexcept : reader_(reader), kind_(reader_kind::group) {}

    [[nodiscard]] reader_kind kind() const noexcept {
        return kind_;
    }

    template<class T>
    std::enable_if_t<std::is_same_v<T, record_reader> || std::is_same_v<T, group_reader>, T*> reader() {
        if (kind_ != to_kind<T>) std::abort();
        return static_cast<T*>(reader_);
    }

private:
    void* reader_{};
    reader_kind kind_{};
};

} // namespace
