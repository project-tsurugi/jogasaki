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

#include <takatori/util/fail.h>

#include <jogasaki/utils/variant.h>
#include <jogasaki/executor/record_reader.h>
#include <jogasaki/executor/group_reader.h>

namespace jogasaki::executor {

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
constexpr inline std::string_view to_string_view(reader_kind value) noexcept {
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

template <class T>
inline constexpr reader_kind to_kind = reader_kind::record;

template <>
inline constexpr reader_kind to_kind<group_reader> = reader_kind::group;

/**
 * @brief reader container to accommodate both record/group readers by type erasure
 */
class reader_container {
public:
    using entity_type = std::variant<std::monostate, record_reader*, group_reader*>;

    template <class T>
    static constexpr std::size_t index_of = alternative_index<T, entity_type>();

    /**
     * @brief create empty instance
     */
    constexpr reader_container() = default;
    explicit reader_container(record_reader* reader) noexcept : reader_(std::in_place_type<record_reader*>, reader) {}
    explicit reader_container(group_reader* reader) noexcept : reader_(std::in_place_type<group_reader*>, reader) {}

    [[nodiscard]] reader_kind kind() const noexcept {
        switch(reader_.index()) {
            case index_of<std::monostate>:
                return reader_kind::unknown;
            case index_of<record_reader*>:
                return to_kind<record_reader>;
            case index_of<group_reader*>:
                return to_kind<group_reader>;
        }
        takatori::util::fail();
    }

    template<class T>
    T* reader() {
        return std::get<T*>(reader_);
    }

    explicit operator bool() const noexcept {
        switch(reader_.index()) {
            case index_of<std::monostate>:
                return false;
            case index_of<record_reader*>:
                return *std::get_if<record_reader*>(&reader_) != nullptr;
            case index_of<group_reader*>:
                return *std::get_if<group_reader*>(&reader_) != nullptr;
        }
        takatori::util::fail();
    }

private:
    entity_type reader_{};
};

} // namespace
