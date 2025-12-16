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
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <jogasaki/utils/sanitize_utf8.h>

namespace jogasaki::utils {

using namespace std::string_view_literals;

class sanitize_utf8_test : public ::testing::Test {};

TEST_F(sanitize_utf8_test, printable_ascii) {
    std::string input("Hello, World!");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("Hello, World!", result);
}

TEST_F(sanitize_utf8_test, alphanumeric_and_symbols) {
    std::string input("ABC123xyz!@#$%^&*()");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("ABC123xyz!@#$%^&*()", result);
}

TEST_F(sanitize_utf8_test, null_byte) {
    std::string input("\u0000"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{00}", result);
}

TEST_F(sanitize_utf8_test, control_characters) {
    std::string input("\u0001\u0002\u0003"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{01}\\x{02}\\x{03}", result);
}

TEST_F(sanitize_utf8_test, tab_newline_carriage_return) {
    std::string input("\t\n\r");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{09}\\x{0a}\\x{0d}", result);
}

TEST_F(sanitize_utf8_test, mixed_printable_and_nonprintable) {
    std::string input("A\u0000B\u0001C"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("A\\x{00}B\\x{01}C", result);
}

TEST_F(sanitize_utf8_test, high_byte_values) {
    std::string input("\xff\xfe\xfd"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{ff}\\x{fe}\\x{fd}", result);
}

TEST_F(sanitize_utf8_test, multiple_consecutive_nonprintable) {
    std::string input("\u0000\u0001\u0002\u0003\u0004"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{00}\\x{01}\\x{02}\\x{03}\\x{04}", result);
}

TEST_F(sanitize_utf8_test, empty_string) {
    std::string input("");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("", result);
}

TEST_F(sanitize_utf8_test, space_character) {
    std::string input(" ");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ(" ", result);
}

TEST_F(sanitize_utf8_test, backslash_character) {
    std::string input("\\");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\", result);
}

TEST_F(sanitize_utf8_test, mixed_with_spaces) {
    std::string input("Hello\u0000 World\u0001!"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("Hello\\x{00} World\\x{01}!", result);
}

// UTF-8 validation tests

TEST_F(sanitize_utf8_test, valid_utf8_2byte) {
    // Valid 2-byte UTF-8: ¢ (U+00A2)
    std::string input("\xC2\xA2");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\xC2\xA2", result);
}

TEST_F(sanitize_utf8_test, valid_utf8_3byte) {
    // Valid 3-byte UTF-8: あ (U+3042)
    std::string input("\xE3\x81\x82");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\xE3\x81\x82", result);
}

TEST_F(sanitize_utf8_test, valid_utf8_4byte) {
    // Valid 4-byte UTF-8: 𠀋 (U+2000B)
    std::string input("\xF0\xA0\x80\x8B");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\xF0\xA0\x80\x8B", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_incomplete_2byte) {
    // Incomplete 2-byte sequence
    std::string input("\xC2"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{c2}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_incomplete_3byte) {
    // Incomplete 3-byte sequence
    std::string input("\xE3\x81"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{e3}\\x{81}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_incomplete_4byte) {
    // Incomplete 4-byte sequence
    std::string input("\xF0\xA0\x80"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{f0}\\x{a0}\\x{80}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_bad_continuation) {
    // Invalid continuation byte
    std::string input("\xC2\x00"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{c2}\\x{00}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_overlong_2byte) {
    // Overlong encoding of 'A' (U+0041) in 2 bytes
    std::string input("\xC1\x81"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{c1}\\x{81}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_overlong_3byte) {
    // Overlong encoding in 3 bytes
    std::string input("\xE0\x80\x80"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{e0}\\x{80}\\x{80}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_overlong_4byte) {
    // Overlong encoding in 4 bytes
    std::string input("\xF0\x80\x80\x80"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{f0}\\x{80}\\x{80}\\x{80}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_surrogate) {
    // UTF-16 surrogate (U+D800) - invalid in UTF-8
    std::string input("\xED\xA0\x80"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{ed}\\x{a0}\\x{80}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_beyond_unicode) {
    // Code point beyond valid Unicode range (> U+10FFFF)
    std::string input("\xF4\x90\x80\x80"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{f4}\\x{90}\\x{80}\\x{80}", result);
}

TEST_F(sanitize_utf8_test, invalid_utf8_start_byte) {
    // Invalid UTF-8 start byte (0xFE)
    std::string input("\xFE"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("\\x{fe}", result);
}

TEST_F(sanitize_utf8_test, mixed_valid_invalid_utf8) {
    // Mix of valid ASCII, valid UTF-8, and invalid bytes
    std::string input("Hello\xC2\xA2World\xFF!"sv);
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("Hello\xC2\xA2World\\x{ff}!", result);
}

TEST_F(sanitize_utf8_test, valid_utf8_with_control_chars) {
    // Valid UTF-8 but contains control characters
    std::string input("Hello\xC2\x80World"sv); // U+0080 is a control character
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("Hello\\x{c2}\\x{80}World", result);
}

TEST_F(sanitize_utf8_test, mixed_japanese_and_ascii) {
    // Japanese hiragana mixed with ASCII
    std::string input("こんにちは世界"); // "Hello World" in Japanese
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("こんにちは世界", result);
}

TEST_F(sanitize_utf8_test, emoji) {
    // Emoji (4-byte UTF-8)
    std::string input("Hello 😀 World");
    std::string result = sanitize_utf8(input);
    EXPECT_EQ("Hello 😀 World", result);
}

}
