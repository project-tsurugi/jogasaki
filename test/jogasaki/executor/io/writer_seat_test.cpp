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
#include <jogasaki/executor/io/writer_seat.h>

#include <memory>
#include <gtest/gtest.h>

#include <jogasaki/executor/io/record_channel.h>
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

TEST(writer_seat_test, reserved_and_writer_creation) {
    fake_record_channel channel{};
    writer_seat seat(&channel, true);

    EXPECT_TRUE(seat.reserved());
    EXPECT_TRUE(! seat.has_writer());

    auto const& wrt = seat.writer();
    EXPECT_TRUE(seat.has_writer());
    EXPECT_TRUE(wrt != nullptr);
    EXPECT_EQ(channel.writers_.size(), 1u);

    auto const& wrt2 = seat.writer();
    EXPECT_EQ(wrt.get(), wrt2.get());
}

TEST(writer_seat_test, move_transfer_preserves_writer) {
    fake_record_channel channel{};
    writer_seat seat(&channel, true);
    auto const& wrt = seat.writer();
    EXPECT_TRUE(seat.has_writer());

    writer_seat moved(std::move(seat));
    EXPECT_TRUE(moved.reserved());
    EXPECT_TRUE(moved.has_writer());
    EXPECT_TRUE(! seat.reserved());
    EXPECT_TRUE(! seat.has_writer());

    writer_seat assigned;
    assigned = std::move(moved);
    EXPECT_TRUE(assigned.reserved());
    EXPECT_TRUE(assigned.has_writer());
}

TEST(writer_seat_test, default_constructed_is_non_reserved_and_no_writer) {
    writer_seat seat{};
    EXPECT_TRUE(! seat.reserved());
    EXPECT_TRUE(! seat.has_writer());
}

}  // namespace jogasaki::executor::io
