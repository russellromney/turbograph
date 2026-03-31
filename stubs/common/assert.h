#pragma once

#include "common/exception/internal.h"
#include <stdexcept>
#include <string>

namespace lbug {
namespace common {

[[noreturn]] inline void kuAssertFailureInternal(const char* condition_name, const char* file,
    int linenr) {
    throw InternalException(std::string("Assertion failed in file \"") + file +
        "\" on line " + std::to_string(linenr) + ": " + condition_name);
}

#if defined(LBUG_RUNTIME_CHECKS) || !defined(NDEBUG)
#define KU_ASSERT(condition)                                                                       \
    static_cast<bool>(condition) ?                                                                 \
        void(0) :                                                                                  \
        lbug::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__)
#else
#define KU_ASSERT(condition) void(0)
#endif

#define KU_UNREACHABLE                                                                             \
    [[unlikely]] lbug::common::kuAssertFailureInternal("KU_UNREACHABLE", __FILE__, __LINE__)

#define KU_UNUSED(expr) (void)(expr)

} // namespace common
} // namespace lbug
