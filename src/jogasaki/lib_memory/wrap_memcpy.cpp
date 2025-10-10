#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif
// NOLINTBEGIN(cert-dcl37-c,cert-dcl51-cpp,bugprone-reserved-identifier)
void* __real_memcpy(void* dest, const void* src, size_t n);

void* __wrap_memcpy(void* dest, const void* src, size_t n) { return __real_memcpy(dest, src, n); }
// NOLINTEND(cert-dcl37-c,cert-dcl51-cpp,bugprone-reserved-identifier)

#ifdef __cplusplus
} // extern "C"
#endif
