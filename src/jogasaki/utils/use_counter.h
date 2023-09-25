/*
 * Copyright 2018-2022 tsurugi project.
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

namespace jogasaki::utils {

/**
 * @brief use counter
 * @details a counter object atomically increase/decrease on copy/destruct
 * @note this object is thread-safe. Multiple threads simultaneously use this object.
 */
class use_counter {
public:
    /**
     * @brief create new counter object with use count = 1
     */
    use_counter();

    /**
     * @brief destruct the counter
     */
    ~use_counter() = default;

    /**
     * @brief copy object and increment the shared use counter
     */
    use_counter(use_counter const& other) = default;

    /**
     * @brief copy assign object and increment the shared use counter
     */
    use_counter& operator=(use_counter const& other) = default;

    /**
     * @brief move construct the object, the newly created object is the simple copy of the `other` and `other` becomes
     * as if default initilized (no counter is shared)
     * @param other source object
     */
    use_counter(use_counter&& other) noexcept;

    /**
     * @brief move assign the object, this object becomes the simple copy of the `other` and `other` becomes
     * as if default initilized (no counter is shared)
     * @param other source object
     */
    use_counter& operator=(use_counter&& other) noexcept;

    /**
     * @brief accessor to the count
     * @details the count tells how many 'copies' of this object exists. Default created object has count 1, and
     * it's incremented when new copy is created. It's decremented when the copy is destructed.
     * @return the use count
     */
    [[nodiscard]] std::size_t count() const noexcept;

    /**
     * @brief reset the object as if it's default created
     * @details the count of this object is reset to 1 and use count of copy of this object is decremented
     */
    void reset();

private:
    std::shared_ptr<int> entity_{};
};

}

