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
#include <cstdint>

#include <jogasaki/utils/fail.h>

namespace jogasaki::utils {

class xorshift_random32 {
public:
    xorshift_random32() = default;

    explicit xorshift_random32(std::uint32_t seed) : x_{seed} {
        if (seed == 0) {
            fail_with_exception();
        }
    }
    std::uint32_t operator()() {
        x_ ^= x_ << 13UL;
        x_ ^= x_ >> 17UL;
        x_ ^= x_ << 5UL;
        return x_;
    }
    void seed(std::uint32_t seed) {
        if (seed == 0) {
            fail_with_exception();
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
            fail_with_exception();
        }
    }
    std::uint64_t operator()() {
        x_ ^= x_ << 13UL;
        x_ ^= x_ >> 7UL;
        x_ ^= x_ << 17UL;
        return x_;
    }
    void seed(std::uint64_t seed) {
        if (seed == 0) {
            fail_with_exception();
        }
        x_ = seed;
    }
private:
    std::uint64_t x_= 88172645463325252ULL;
};

} //namespace

