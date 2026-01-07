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

#include <cstddef>
#include <ostream>
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/data/any.h>

namespace jogasaki::data {

/**
 * @brief represents a sequence of any values (a single row of table-valued function result).
 * @details this class wraps a sequence_view<any> and provides access to the underlying values.
 *          it can hold either a view to external data or own the data internally.
 */
class any_sequence {
public:
    using value_type = any;
    using view_type = takatori::util::sequence_view<value_type const>;
    using storage_type = std::vector<value_type>;
    using size_type = std::size_t;
    using iterator = view_type::iterator;

    /**
     * @brief constructs an empty sequence.
     */
    any_sequence() = default;

    /**
     * @brief destructor.
     */
    ~any_sequence() = default;

    /**
     * @brief copy constructor.
     * @param other the source sequence
     */
    any_sequence(any_sequence const& other);

    /**
     * @brief move constructor.
     * @param other the source sequence
     */
    any_sequence(any_sequence&& other) noexcept;

    /**
     * @brief copy assignment operator.
     * @param other the source sequence
     * @return reference to this
     */
    any_sequence& operator=(any_sequence const& other);

    /**
     * @brief move assignment operator.
     * @param other the source sequence
     * @return reference to this
     */
    any_sequence& operator=(any_sequence&& other) noexcept;

    /**
     * @brief constructs a sequence with external view.
     * @param view the sequence view to wrap
     * @attention the caller must ensure the view remains valid during the lifetime of this object.
     */
    explicit any_sequence(view_type view) noexcept;

    /**
     * @brief constructs a sequence with internal storage.
     * @param values the values to store internally
     */
    explicit any_sequence(storage_type values) noexcept;

    /**
     * @brief constructs a sequence with initializer list.
     * @param values the values to store internally
     */
    any_sequence(std::initializer_list<value_type> values);

    /**
     * @brief returns the number of elements in the sequence.
     * @return the number of elements
     */
    [[nodiscard]] size_type size() const noexcept;

    /**
     * @brief returns whether the sequence is empty.
     * @return true if the sequence is empty
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief returns the element at the specified position.
     * @param index the position of the element
     * @return the element at the specified position
     * @pre index < size()
     */
    [[nodiscard]] value_type const& operator[](size_type index) const noexcept;

    /**
     * @brief returns the underlying sequence view.
     * @return the sequence view
     */
    [[nodiscard]] view_type view() const noexcept;

    /**
     * @brief returns an iterator to the beginning.
     * @return an iterator to the beginning
     */
    [[nodiscard]] iterator begin() const noexcept;

    /**
     * @brief returns an iterator to the end.
     * @return an iterator to the end
     */
    [[nodiscard]] iterator end() const noexcept;

    /**
     * @brief clears the sequence.
     */
    void clear() noexcept;

    /**
     * @brief assigns new values from a sequence view.
     * @param view the sequence view to assign
     * @attention the caller must ensure the view remains valid during the lifetime of this object.
     */
    void assign(view_type view) noexcept;

    /**
     * @brief assigns new values from internal storage.
     * @param values the values to store internally
     */
    void assign(storage_type values) noexcept;

private:
    storage_type storage_{};
};

/**
 * @brief equality comparison operator.
 * @param a the first sequence
 * @param b the second sequence
 * @return true if the sequences are equal
 */
bool operator==(any_sequence const& a, any_sequence const& b) noexcept;

/**
 * @brief inequality comparison operator.
 * @param a the first sequence
 * @param b the second sequence
 * @return true if the sequences are not equal
 */
bool operator!=(any_sequence const& a, any_sequence const& b) noexcept;

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
std::ostream& operator<<(std::ostream& out, any_sequence const& value);

}  // namespace jogasaki::data
