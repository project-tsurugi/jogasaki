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

namespace jogasaki::common_cli {

class xorshift_random {
public:
    xorshift_random() = default;

    explicit xorshift_random(std::uint32_t seed) : x_{seed} {
        if (seed == 0) {
            std::abort();
        }
    }
    std::uint32_t operator()() {
        x_ ^= x_ << 13;
        x_ ^= x_ >> 17;
        x_ ^= x_ << 5;
        return x_;
    }
    void seed(std::uint32_t seed) {
        if (seed == 0) {
            std::abort();
        }
        x_ = seed;
    }
private:
    std::uint32_t x_=123456789;
};

class xorshift_random64 {
public:
    xorshift_random64() = default;

    explicit xorshift_random64(std::uint64_t seed) : x_{seed} {
        if (seed == 0) {
            std::abort();
        }
    }
    std::uint64_t operator()() {
        x_ ^= x_ << 13;
        x_ ^= x_ >> 7;
        x_ ^= x_ << 17;
        return x_;
    }
    void seed(std::uint64_t seed) {
        if (seed == 0) {
            std::abort();
        }
        x_ = seed;
    }
private:
    std::uint64_t x_= 88172645463325252ULL;
};

} //namespace

