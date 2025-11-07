/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/executor/io/writer_pool.h>

#include <memory>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/io/writer_seat.h>
#include <jogasaki/status.h>

namespace jogasaki::executor::io {

class fake_record_writer : public record_writer {
public:
    bool write(accessor::record_ref) override { return true; }
    void flush() override {}
    void release() override { released_ = true; }
    [[nodiscard]] bool is_released() const noexcept { return released_; }
private:
    bool released_{};
};

class fake_record_channel : public record_channel {
public:
    status acquire(std::shared_ptr<record_writer>& wrt) override {
        auto w = std::make_shared<fake_record_writer>();
        writers_.push_back(w);
        wrt = w;
        return status::ok;
    }

    status meta(takatori::util::maybe_shared_ptr<meta::external_record_meta>) override {
        return status::ok;
    }

    record_channel_stats& statistics() override { return stats_; }

    [[nodiscard]] record_channel_kind kind() const noexcept override {
        return record_channel_kind::null_record_channel;
    }
    [[nodiscard]] std::optional<std::size_t> max_writer_count() override {
        return {};
    }

    std::vector<std::shared_ptr<fake_record_writer>> writers_{};
    record_channel_stats stats_{};
};

TEST(writer_pool_test, acquire_release_and_capacity) {
    fake_record_channel channel{};
    writer_pool pool{channel, 2};
    EXPECT_EQ(pool.capacity(), 2u);

    writer_seat s1{}, s2{}, s3{};
    EXPECT_TRUE(pool.acquire(s1));
    EXPECT_TRUE(pool.acquire(s2));
    EXPECT_TRUE(! pool.acquire(s3));

    // lazily create writer for s1
    auto const& w1 = s1.writer();
    EXPECT_TRUE(s1.has_writer());
    EXPECT_EQ(channel.writers_.size(), 1u);

    // return seat and re-acquire
    pool.release(std::move(s1));
    EXPECT_TRUE(pool.acquire(s1));

    // return the other held seat so another acquire succeeds
    pool.release(std::move(s2));

    // acquire seat and create writer to exercise release_pool
    writer_seat temp;
    EXPECT_TRUE(pool.acquire(temp));
    temp.writer();

    // return held seats so the pool can clean up their writers
    pool.release(std::move(s1));
    pool.release(std::move(temp));

    // release_pool must call release() on held writers
    pool.release_pool();

    for (auto const& w : channel.writers_) {
        EXPECT_TRUE(w->is_released());
    }
}

}  // namespace jogasaki::executor::io
