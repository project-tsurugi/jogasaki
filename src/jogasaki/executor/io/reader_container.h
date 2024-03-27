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

#include <cstdlib>
#include <iosfwd>
#include <memory>
#include <string_view>
#include <variant>

#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/record_reader.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/variant.h>

namespace jogasaki::executor::io {

/**
 * @brief reader kind
 */
enum class reader_kind {
    unknown,
    record,
    group,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(reader_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = reader_kind;
    switch (value) {
        case kind::unknown: return "unknown"sv;
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

namespace details {

template <class T>
inline constexpr reader_kind to_kind = reader_kind::record;

template <>
inline constexpr reader_kind to_kind<group_reader> = reader_kind::group;

}

/**
 * @brief reader container to accommodate both record/group readers by type erasure
 */
class reader_container {
public:
    /// @brief entity type
    using entity_type = std::variant<std::monostate, record_reader*, group_reader*>;

    /**
     * @brief create empty instance
     */
    constexpr reader_container() = default;

    /**
     * @brief create new instance holding record reader
     * @param reader the object to hold
     */
    explicit reader_container(record_reader* reader) noexcept;

    /**
     * @brief create new instance holding group reader
     * @param reader the object to hold
     */
    explicit reader_container(group_reader* reader) noexcept;

    /**
     * @brief getter for the reader kind
     * @return kind of the reader held by this object
     */
    [[nodiscard]] reader_kind kind() const noexcept;

    /**
     * @brief getter for the reader
     * @tparam T type of the reader
     * @return pointer to the reader held by this object
     */
    template<class T>
    [[nodiscard]] T* reader() {
        return std::get<T*>(reader_);
    }

    /**
     * @brief getter of the validity
     * @return whether the container holds any reader or not
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    void release();
private:
    entity_type reader_{};

    template <class T>
    static constexpr std::size_t index_of = alternative_index<T, entity_type>();
};

}  // namespace jogasaki::executor::io
