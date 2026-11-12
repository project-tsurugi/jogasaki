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
#pragma once
#include <string_view>
namespace jogasaki::testing {
// Public test certificate; only X.509 syntax is validated, not its validity dates.
inline constexpr std::string_view remote_test_root_certificate = R"CERT(-----BEGIN CERTIFICATE-----
MIIDLTCCAhWgAwIBAgIUVKnMq2wFY4gIminqIKP0zcOl7H4wDQYJKoZIhvcNAQEL
BQAwJjEkMCIGA1UEAwwbZGlzdHJpYnV0ZWQtcXVlcnktdW5pdC10ZXN0MB4XDTI2
MTExMjE5NDUxOFoXDTI2MTExMzE5NDUxOFowJjEkMCIGA1UEAwwbZGlzdHJpYnV0
ZWQtcXVlcnktdW5pdC10ZXN0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAylyizpVv7VlmEcCDSrleOoHy2FVUrd0ua4G7VoLF/iLjUHjrahmT0gPZ+NFh
KtXOteM8fBG8Munls1SUDmEhjJSQzr+DxF68Dw1WOTy1SyyVMa6Hk/dcUSrk5Zz/
/jRW79FlO/hlh0LE/9hx5dOzTPHBQuNGrsP1vBL0XUIAUvFZhIA5TYt7ZrNx5HeQ
dm6B5aN11obRbhR+YVwrTeQ+ZctqC1C9PdMKilddX4nihlTCiTtM7WAvwqM7GmUh
tIBvnXaxwhryYW3GkjbJEW7EdzBTPrFGWhidtYZKvpGbdYXn9i5d3ghsGXXuZLKo
TsvLP6JXUtS352bH+Zp1gIHQvQIDAQABo1MwUTAdBgNVHQ4EFgQUC42T2j6OumIm
Czv5OY3WZheg3uIwHwYDVR0jBBgwFoAUC42T2j6OumImCzv5OY3WZheg3uIwDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAv+L80rxv1junO3jzOBzT
Yi3n0rxvc4lKF9TOlNUUXNLXJ8CJqv0lVSlFkXVpKePsYe/sA+QkcwauFBzJ1CW/
cXfmbCMs4YXgVhdJSFayQ/47L/a8I725F7Y2CdTQtU056BiA1e8l7dmeZ6Bwffoq
KVgGi6F4bm2oUL9P7RehDUayypXItTU/3gnMEtlEYLZHs2e/Wf+TC+w+I7tpzNbU
GidBrJeXPDPWmkCqqBWJPBDl5/jrAG/XjI7ChxECWL0RFHj8O/XIy6uhApuG/J7I
SFmnHZdSOWPUbiJEzHnUz/16Q13OcNh4NQa/uFq7FoNVF12sd3kByxZS2zlhHOLu
sg==
-----END CERTIFICATE-----
)CERT";
}
