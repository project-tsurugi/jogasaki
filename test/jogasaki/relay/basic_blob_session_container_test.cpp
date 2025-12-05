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
#include <memory>
#include <gtest/gtest.h>

#include <jogasaki/relay/blob_session_container.h>

#include "../test_root.h"

namespace jogasaki::relay {

/**
 * @brief mock blob session for testing
 * @details This mock session tracks dispose() calls for testing purposes.
 * The container only calls dispose() but does not own the memory.
 * Tests must ensure proper cleanup of mock_session instances.
 */
class mock_session {
public:
    /**
     * @brief create new object
     */
    mock_session() = default;

    /**
     * @brief destruct the object
     */
    ~mock_session() = default;

    mock_session(mock_session const& other) = delete;
    mock_session& operator=(mock_session const& other) = delete;
    mock_session(mock_session&& other) noexcept = delete;
    mock_session& operator=(mock_session&& other) noexcept = delete;

    /**
     * @brief dispose the session
     * @details marks the session as disposed for testing verification
     */
    void dispose() {
        disposed_ = true;
    }

    /**
     * @brief check if the session has been disposed
     * @return true if disposed, false otherwise
     */
    [[nodiscard]] bool is_disposed() const noexcept {
        return disposed_;
    }

private:
    bool disposed_{false};
};

// testcase to verify the non-specilized part of the blob_session_container,
// that is, covering member functions other than get_or_create(), which is specific to template prod. blob_session impl.
class basic_blob_session_container_test : public test_root {};

TEST_F(basic_blob_session_container_test, default_constructor) {
    basic_blob_session_container<mock_session> container{};

    EXPECT_TRUE(! container);
    EXPECT_TRUE(! container.has_session());
    EXPECT_EQ(nullptr, container.get());
}

TEST_F(basic_blob_session_container_test, constructor_with_session) {
    auto session = std::make_unique<mock_session>();
    auto* p = session.get();

    basic_blob_session_container<mock_session> container{};
    container.set(p);

    EXPECT_TRUE(container);
    EXPECT_TRUE(container.has_session());
    EXPECT_EQ(p, container.get());
}

TEST_F(basic_blob_session_container_test, dispose_manually) {
    auto session = std::make_unique<mock_session>();
    auto* p = session.get();

    basic_blob_session_container<mock_session> container{};
    container.set(p);
    ASSERT_TRUE(container);
    ASSERT_TRUE(! p->is_disposed());

    container.dispose();

    EXPECT_TRUE(! container);
    EXPECT_TRUE(! container.has_session());
    EXPECT_EQ(nullptr, container.get());
    EXPECT_TRUE(p->is_disposed());
}

TEST_F(basic_blob_session_container_test, dispose_idempotent) {
    auto session = std::make_unique<mock_session>();
    auto* p = session.get();

    basic_blob_session_container<mock_session> container{};
    container.set(p);
    ASSERT_TRUE(container);

    container.dispose();
    ASSERT_TRUE(p->is_disposed());

    // second dispose should be no-op
    container.dispose();

    EXPECT_TRUE(! container);
    EXPECT_TRUE(! container.has_session());
    EXPECT_EQ(nullptr, container.get());
}

TEST_F(basic_blob_session_container_test, destructor_disposes_session) {
    auto session = std::make_unique<mock_session>();
    auto* p = session.get();

    {
        basic_blob_session_container<mock_session> container{};
        container.set(p);
        ASSERT_TRUE(container);
        ASSERT_TRUE(! p->is_disposed());
    }

    // session should be disposed by destructor
    EXPECT_TRUE(p->is_disposed());
}

TEST_F(basic_blob_session_container_test, set_after_dispose) {
    auto session1 = std::make_unique<mock_session>();
    auto* p1 = session1.get();
    auto session2 = std::make_unique<mock_session>();
    auto* p2 = session2.get();

    basic_blob_session_container<mock_session> container{};
    container.set(p1);
    ASSERT_TRUE(container);
    ASSERT_EQ(p1, container.get());

    container.dispose();
    ASSERT_TRUE(p1->is_disposed());
    ASSERT_TRUE(! container);

    container.set(p2);

    EXPECT_TRUE(container);
    EXPECT_TRUE(container.has_session());
    EXPECT_EQ(p2, container.get());
    EXPECT_TRUE(! p2->is_disposed());
}

TEST_F(basic_blob_session_container_test, set_on_empty_container) {
    auto session = std::make_unique<mock_session>();
    auto* p = session.get();

    basic_blob_session_container<mock_session> container{};
    ASSERT_TRUE(! container);

    container.set(p);

    EXPECT_TRUE(container);
    EXPECT_TRUE(container.has_session());
    EXPECT_EQ(p, container.get());
}

TEST_F(basic_blob_session_container_test, set_nullptr_after_dispose) {
    auto session = std::make_unique<mock_session>();
    auto* p = session.get();

    basic_blob_session_container<mock_session> container{};
    container.set(p);
    ASSERT_TRUE(container);

    container.dispose();
    ASSERT_TRUE(p->is_disposed());

    container.set(nullptr);

    EXPECT_TRUE(! container);
    EXPECT_TRUE(! container.has_session());
    EXPECT_EQ(nullptr, container.get());
}

TEST_F(basic_blob_session_container_test, dispose_on_empty_container) {
    basic_blob_session_container<mock_session> container{};
    ASSERT_TRUE(! container);

    // dispose on empty container should be no-op
    container.dispose();

    EXPECT_TRUE(! container);
    EXPECT_TRUE(! container.has_session());
    EXPECT_EQ(nullptr, container.get());
}

TEST_F(basic_blob_session_container_test, bool_conversion_operator) {
    auto session = std::make_unique<mock_session>();

    basic_blob_session_container<mock_session> container{};
    EXPECT_TRUE(! static_cast<bool>(container));

    container.set(session.get());
    EXPECT_TRUE(static_cast<bool>(container));

    container.dispose();
    EXPECT_TRUE(! static_cast<bool>(container));
}

}
