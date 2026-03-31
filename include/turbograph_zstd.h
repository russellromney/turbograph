#pragma once

// Compatibility shim: ladybug-fork vendors zstd in lbug_zstd:: namespace,
// standalone builds use system zstd with no namespace.
#include <zstd.h>

// If lbug_zstd namespace exists (ladybug-fork vendored build), import it.
// The vendored zstd.h wraps all symbols in namespace lbug_zstd {}.
#ifdef LBUG_ZSTD_NAMESPACE
using namespace lbug_zstd;
#endif
