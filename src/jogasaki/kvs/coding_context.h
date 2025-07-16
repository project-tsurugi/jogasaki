/*
 * Copyright 2018-2024 Project Tsurugi.
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

namespace jogasaki::kvs {

/**
 * @brief context for encoding/decoding
 */
class coding_context {
public:
    /**
     * @brief create default coding context
     */
    coding_context() = default;

    /**
     * @brief returns whether the encoding request is to write
     */
    [[nodiscard]] bool coding_for_write() const noexcept {
        return coding_for_write_;
    }

    /**
     * @brief setter for varlen info
     */
    void coding_for_write(bool arg) noexcept {
        coding_for_write_ = arg;
    }

private:
    bool coding_for_write_{false};

};

}  // namespace jogasaki::kvs
