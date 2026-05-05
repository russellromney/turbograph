#pragma once

#if __has_include("common/types/ku_string.h")
#include "common/types/ku_string.h"
namespace lbug {
namespace common {
using turbograph_string_t = ku_string_t;
} // namespace common
} // namespace lbug
#elif __has_include("common/types/string_t.h")
#include "common/types/string_t.h"
namespace lbug {
namespace common {
using turbograph_string_t = string_t;
} // namespace common
} // namespace lbug
#else
#error "Turbograph requires a Ladybug string type header"
#endif
