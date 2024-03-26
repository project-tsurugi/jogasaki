#include <initializer_list>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <gtest/gtest.h>

#include <jogasaki/serializer/base128v.h>


namespace jogasaki::serializer::base128v {

using takatori::util::buffer_view;

class base128v_test : public ::testing::Test {
public:
    std::string str(std::initializer_list<unsigned char> list) {
        std::string result {};
        result.resize(list.size());
        for (std::size_t i = 0, n = list.size(); i < n; ++i) {
            result[i] = static_cast<std::string::value_type>(*(list.begin() + i));
        }
        return result;
    }

    std::string dump_unsigned(std::uint64_t value) {
        std::string result {};
        result.resize(9);
        buffer_view view { result.data(), result.size() };
        auto iter = view.begin();
        if (write_unsigned(value, iter, view.end())) {
            result.resize(std::distance(view.begin(), iter));
            return result;
        }
        return {};
    }

    std::optional<std::uint64_t> restore_unsigned(std::string sequence) {
        buffer_view view { sequence.data(), sequence.size() };
        auto iter = view.begin();
        if (auto result = read_unsigned(iter, view.end())) {
            if (iter != view.end()) {
                throw std::domain_error("rest bytes");
            }
            return result;
        }
        return std::nullopt;
    }

    bool validate_unsigned(std::uint64_t value) {
        auto dump = dump_unsigned(value);
        auto restored = restore_unsigned(std::move(dump));
        return restored == value;
    }

    std::string dump_signed(std::uint64_t value) {
        std::string result {};
        result.resize(9);
        buffer_view view { result.data(), result.size() };
        auto iter = view.begin();
        if (write_signed(value, iter, view.end())) {
            result.resize(std::distance(view.begin(), iter));
            return result;
        }
        return {};
    }

    std::optional<std::uint64_t> restore_signed(std::string sequence) {
        buffer_view view { sequence.data(), sequence.size() };
        auto iter = view.begin();
        if (auto result = read_signed(iter, view.end())) {
            if (iter != view.end()) {
                throw std::domain_error("rest bytes");
            }
            return result;
        }
        return std::nullopt;
    }

    bool validate_signed(std::uint64_t value) {
        auto dump = dump_signed(value);
        auto restored = restore_signed(std::move(dump));
        return restored == value;
    }
};

TEST_F(base128v_test, estimate_unsigned) {
    EXPECT_EQ(size_unsigned(0ULL), 1);
    EXPECT_EQ(size_unsigned(1ULL), 1);
    EXPECT_EQ(size_unsigned((1ULL << 7U) - 1), 1);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 1)) + 0), 2);
    EXPECT_EQ(size_unsigned((1ULL << (7U * 2)) - 1), 2);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 2)) + 0), 3);
    EXPECT_EQ(size_unsigned((1ULL << (7U * 3)) - 1), 3);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 3)) + 0), 4);
    EXPECT_EQ(size_unsigned((1ULL << (7U * 4)) - 1), 4);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 4)) + 0), 5);
    EXPECT_EQ(size_unsigned((1ULL << (7U * 5)) - 1), 5);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 5)) + 0), 6);
    EXPECT_EQ(size_unsigned((1ULL << (7U * 6)) - 1), 6);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 6)) + 0), 7);
    EXPECT_EQ(size_unsigned((1ULL << (7U * 7)) - 1), 7);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 7)) + 0), 8);
    EXPECT_EQ(size_unsigned((1ULL << (7U * 8)) - 1), 8);

    EXPECT_EQ(size_unsigned((1ULL << (7U * 8)) + 0), 9);
    EXPECT_EQ(size_unsigned(std::numeric_limits<std::uint64_t>::max()), 9);
}

TEST_F(base128v_test, write_unsigned) {
    EXPECT_EQ(dump_unsigned(0), str({ 0 }));
    EXPECT_EQ(dump_unsigned(1ULL), str({ 1 }));
    EXPECT_EQ(dump_unsigned((1ULL << 7U) - 1), str({ 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 1)) + 0), str({ 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned((1ULL << (7U * 2)) - 1), str({ 0xff, 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 2)) + 0), str({ 0x80, 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned((1ULL << (7U * 3)) - 1), str({ 0xff, 0xff, 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 3)) + 0), str({ 0x80, 0x80, 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned((1ULL << (7U * 4)) - 1), str({ 0xff, 0xff, 0xff, 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 4)) + 0), str({ 0x80, 0x80, 0x80, 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned((1ULL << (7U * 5)) - 1), str({ 0xff, 0xff, 0xff, 0xff, 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 5)) + 0), str({ 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned((1ULL << (7U * 6)) - 1), str({ 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 6)) + 0), str({ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned((1ULL << (7U * 7)) - 1), str({ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 7)) + 0), str({ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned((1ULL << (7U * 8)) - 1), str({ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f }));

    EXPECT_EQ(dump_unsigned((1ULL << (7U * 8)) + 0),
            str({ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 }));
    EXPECT_EQ(dump_unsigned(std::numeric_limits<std::uint64_t>::max()),
            str({ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }));
}

TEST_F(base128v_test, read_unsigned) {
    EXPECT_TRUE(validate_unsigned(0));
    EXPECT_TRUE(validate_unsigned(1ULL));
    EXPECT_TRUE(validate_unsigned((1ULL << 7U) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 1)) + 0));
    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 2)) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 2)) + 0));
    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 3)) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 3)) + 0));
    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 4)) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 4)) + 0));
    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 5)) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 5)) + 0));
    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 6)) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 6)) + 0));
    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 7)) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 7)) + 0));
    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 8)) - 1));

    EXPECT_TRUE(validate_unsigned((1ULL << (7U * 8)) + 0));
    EXPECT_TRUE(validate_unsigned(std::numeric_limits<std::uint64_t>::max()));
}

TEST_F(base128v_test, estimate_signed) {
    EXPECT_EQ(size_signed(0), 1);
    EXPECT_EQ(size_signed(+1), 1);
    EXPECT_EQ(size_signed(-1), 1);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 6ULL) - 1), 1);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 6ULL) - 0), 1);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 6ULL) - 0), 2);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 6ULL) - 1), 2);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 13ULL) - 1), 2);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 13ULL) - 0), 2);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 13ULL) - 0), 3);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 13ULL) - 1), 3);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 20ULL) - 1), 3);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 20ULL) - 0), 3);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 20ULL) - 0), 4);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 20ULL) - 1), 4);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 27ULL) - 1), 4);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 27ULL) - 0), 4);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 27ULL) - 0), 5);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 27ULL) - 1), 5);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 34ULL) - 1), 5);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 34ULL) - 0), 5);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 34ULL) - 0), 6);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 34ULL) - 1), 6);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 41ULL) - 1), 6);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 41ULL) - 0), 6);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 41ULL) - 0), 7);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 41ULL) - 1), 7);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 48ULL) - 1), 7);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 48ULL) - 0), 7);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 48ULL) - 0), 8);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 48ULL) - 1), 8);
    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 55ULL) - 1), 8);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 55ULL) - 0), 8);

    EXPECT_EQ(size_signed(+static_cast<std::int64_t>(1ULL << 55ULL) - 0), 9);
    EXPECT_EQ(size_signed(-static_cast<std::int64_t>(1ULL << 55ULL) - 1), 9);
    EXPECT_EQ(size_signed(std::numeric_limits<std::int64_t>::max()), 9);
    EXPECT_EQ(size_signed(std::numeric_limits<std::int64_t>::min()), 9);
}

TEST_F(base128v_test, read_write_signed) {
    EXPECT_TRUE(validate_signed(0));
    EXPECT_TRUE(validate_signed(+1));
    EXPECT_TRUE(validate_signed(-1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 6ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 6ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 6ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 6ULL) - 1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 13ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 13ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 13ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 13ULL) - 1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 20ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 20ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 20ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 20ULL) - 1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 27ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 27ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 27ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 27ULL) - 1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 34ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 34ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 34ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 34ULL) - 1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 41ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 41ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 41ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 41ULL) - 1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 48ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 48ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 48ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 48ULL) - 1));
    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 55ULL) - 1));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 55ULL) - 0));

    EXPECT_TRUE(validate_signed(+static_cast<std::int64_t>(1ULL << 55ULL) - 0));
    EXPECT_TRUE(validate_signed(-static_cast<std::int64_t>(1ULL << 55ULL) - 1));
    EXPECT_TRUE(validate_signed(std::numeric_limits<std::int64_t>::max()));
    EXPECT_TRUE(validate_signed(std::numeric_limits<std::int64_t>::min()));
}

TEST_F(base128v_test, write_overflow) {
    std::string buf {};
    {
        buf.resize(0);
        buffer_view view { buf.data(), buf.size() };
        auto iter = view.begin();
        EXPECT_FALSE(write_unsigned(1, iter, view.end()));
        EXPECT_EQ(iter, view.begin());
    }
    {
        buf.resize(8);
        buffer_view view { buf.data(), buf.size() };
        auto iter = view.begin();
        EXPECT_FALSE(write_unsigned(std::numeric_limits<std::uint64_t>::max(), iter, view.end()));
        EXPECT_EQ(iter, view.begin());
    }
}

TEST_F(base128v_test, read_undeflow) {
    {
        auto buf = dump_unsigned(0);
        buffer_view view { buf.data(), buf.size() - 1};
        auto iter = view.begin();
        EXPECT_FALSE(read_unsigned(iter, view.end()));
        EXPECT_EQ(iter, view.begin());
    }
    {
        auto buf = dump_unsigned(std::numeric_limits<std::uint64_t>::max());
        buffer_view view { buf.data(), buf.size() - 1};
        auto iter = view.begin();
        EXPECT_FALSE(read_unsigned(iter, view.end()));
        EXPECT_EQ(iter, view.begin());
    }
}

} // namespace jogasaki::serializer::base128v
