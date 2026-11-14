/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "remote_server_configuration_validation.h"

#include <arpa/inet.h>
#include <charconv>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <jogasaki/utils/finally.h>

namespace jogasaki::dist {
std::string_view remote_server_uri_scheme(std::string_view uri) noexcept {
    if (uri.substr(0, 4) == "dns:") return "dns";
    if (uri.substr(0, 5) == "unix:") return "unix";
    return "other";
}

bool valid_remote_server_uri(std::string_view uri, bool secure) noexcept {
    if (uri.find('\0') != std::string_view::npos) return false;
    constexpr std::string_view unix_prefix{"unix://"};
    if (uri.substr(0, unix_prefix.size()) == unix_prefix) {
        auto path = uri.substr(unix_prefix.size());
        return !secure && !path.empty() && path.front() == '/' && path.find_first_of("?#") == std::string_view::npos;
    }
    constexpr std::string_view dns_prefix{"dns:///"};
    if (uri.substr(0, dns_prefix.size()) != dns_prefix) return false;
    auto target = uri.substr(dns_prefix.size());
    if (target.empty() || target.find_first_of("/\\@,;|?#%") != std::string_view::npos) return false;
    for (unsigned char c : target) if (c <= 0x20U || c == 0x7fU) return false;
    std::string_view port{};
    if (target.front() == '[') {
        auto end = target.find(']');
        if (end == std::string_view::npos || end + 1 >= target.size() || target[end + 1] != ':') return false;
        auto host = target.substr(1, end - 1);
        // The literal is bounded by the maximum IPv6 text representation; no allocation is required.
        if (host.empty() || host.size() >= INET6_ADDRSTRLEN) return false;
        char text[INET6_ADDRSTRLEN]{};
        host.copy(text, host.size());
        in6_addr address{};
        if (::inet_pton(AF_INET6, text, &address) != 1) return false;
        port = target.substr(end + 2);
    } else {
        auto colon = target.find(':');
        if (colon == std::string_view::npos || colon == 0 || target.find_first_of("[]") != std::string_view::npos) return false;
        port = target.substr(colon + 1);
    }
    if (port.empty()) return false;
    unsigned int number{};
    auto [end, error] = std::from_chars(port.data(), port.data() + port.size(), number);
    return error == std::errc{} && end == port.data() + port.size() && number >= 1 && number <= 65535;
}

bool valid_remote_root_certificates(std::string_view pem) {
    constexpr std::string_view begin{"-----BEGIN CERTIFICATE-----"};
    constexpr std::string_view end{"-----END CERTIFICATE-----"};
    std::size_t offset{};
    bool found{};
    // Configuration validation must not leave parser errors for subsequent TLS operations.
    auto marked = ERR_set_mark() == 1;
    utils::finally clear_errors{[marked] { if (marked) ERR_pop_to_mark(); else ERR_clear_error(); }};
    while (true) {
        auto start = pem.find("-----BEGIN ", offset);
        if (start == std::string_view::npos) return found;
        if ((start != 0 && pem[start - 1] != '\n') || pem.substr(start, begin.size()) != begin) return false;
        auto finish = pem.find(end, start + begin.size());
        if (finish == std::string_view::npos) return false;
        auto after = finish + end.size();
        while (after < pem.size() && (pem[after] == ' ' || pem[after] == '\t' || pem[after] == '\r')) ++after;
        if (after < pem.size() && pem[after] != '\n') return false;
        auto block = pem.substr(start, finish + end.size() - start);
        if (block.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) return false;
        auto bio = std::unique_ptr<BIO, decltype(&BIO_free)>{BIO_new_mem_buf(block.data(), static_cast<int>(block.size())), BIO_free};
        if (!bio) return false;
        auto certificate = std::unique_ptr<X509, decltype(&X509_free)>{PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr), X509_free};
        if (!certificate) return false;
        found = true;
        offset = finish + end.size();
    }
}

bool valid_remote_server_credentials(remote_server_configuration const& configuration) {
    if (configuration.secure && configuration.uri.substr(0, 5) == "unix:") return false;
    if (!configuration.secure) {
        return configuration.root_certificate == root_certificate_source::none && configuration.pem_root_certs.empty();
    }
    switch (configuration.root_certificate) {
        case root_certificate_source::grpc_default_roots: return configuration.pem_root_certs.empty();
        case root_certificate_source::pem: return valid_remote_root_certificates(configuration.pem_root_certs);
        case root_certificate_source::none: return false;
    }
    return false;
}
} // namespace jogasaki::dist
