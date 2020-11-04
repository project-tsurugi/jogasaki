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

#include <glog/logging.h>
#include <takatori/util/fail.h>
#include <sharksfin/api.h>

namespace jogasaki::kvs {

using ::takatori::util::fail;

class database;

/**
 * @brief iterator object
 */
class iterator {
public:
    /**
     * @brief create empty object
     */
    iterator() = default;

    /**
     * @brief create new object
     */
    explicit iterator(sharksfin::IteratorHandle handle) : handle_(handle) {}

    /**
     * @brief destruct object
     */
    ~iterator() noexcept {
        sharksfin::iterator_dispose(handle_);
    }

    iterator(iterator const& other) = delete;
    iterator& operator=(iterator const& other) = delete;
    iterator(iterator&& other) noexcept = delete;
    iterator& operator=(iterator&& other) noexcept = delete;

    /**
     * @brief move the iterator to next entry
     * @return true if the operation is successful
     * @return false otherwise (e.g. next entry is not found)
     */
    [[nodiscard]] bool next() {
        sharksfin::StatusCode res = sharksfin::iterator_next(handle_);
        if (res == sharksfin::StatusCode::OK) {
            return true;
        }
        if (res == sharksfin::StatusCode::NOT_FOUND) {
            return false;
        }
        fail();
    }

    /**
     * @brief retrieve the key of the current iterator position
     * This doesn't change the iterator state.
     * @param k[out] the key of the current entry
     * The data pointed by the returned value gets invalidated if the iterator state is changed.
     * @return true if the operation is successful
     * @return false otherwise
     * @pre next() is called beforehand successfully to place the iterator on correct entry
     */
    [[nodiscard]] bool key(std::string_view& k) const {
        sharksfin::Slice slice{};
        if(sharksfin::StatusCode res = sharksfin::iterator_get_key(handle_, &slice);res != sharksfin::StatusCode::OK) {
            return false;
        }
        k = slice.to_string_view();
        return true;
    }

    /**
     * @brief retrieve the value of the current iterator position
     * This doesn't change the iterator state.
     * @param v[out] the value of the current entry
     * The data pointed by the returned value gets invalidated if the iterator state is changed.
     * @return true if the operation is successful
     * @return false otherwise
     * @pre next() is called beforehand successfully to place the iterator on correct entry
     */
    [[nodiscard]] bool value(std::string_view& v) const {
        sharksfin::Slice slice{};
        if(sharksfin::StatusCode res = sharksfin::iterator_get_value(handle_, &slice);res != sharksfin::StatusCode::OK) {
            return false;
        }
        v = slice.to_string_view();
        return true;
    }

    /**
     * @brief return the native handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::IteratorHandle handle() const noexcept {
        return handle_;
    }
private:
    sharksfin::IteratorHandle handle_{};
};

/**
 * @brief compare contents of two objects
 * @param a first arg to compare
 * @param b second arg to compare
 * @return true if a == b
 * @return false otherwise
 */
inline bool operator==(iterator const& a, iterator const& b) noexcept {
    return a.handle() == b.handle();
}
inline bool operator!=(iterator const& a, iterator const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, iterator const& value) {
    out << "iterator(handle:" << std::hex << value.handle() << ")";
    return out;
}

}

