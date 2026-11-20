/* ADIOS2 2.12.0.1001's bundled dill uses the macOS cache API on Linux ARM64.
 * Supply the equivalent GCC/Clang operation while building that PyPI package.
 * Keep streaming/JIT support enabled; do not suppress the compiler diagnostic.
 */
#if defined(__linux__) && defined(__aarch64__)
#include <stddef.h>

static inline void hpc_campaign_icache_invalidate(void *start, size_t length)
{
    __builtin___clear_cache((char *)start, (char *)start + length);
}

#define sys_icache_invalidate(start, length) \
    hpc_campaign_icache_invalidate((start), (length))
#endif
