#pragma once

// Minimal LBUG_API stub for standalone builds.
#ifdef LBUG_STATIC_DEFINE
#define LBUG_API
#else
#ifndef LBUG_API
#ifdef LBUG_EXPORTS
#define LBUG_API __attribute__((visibility("default")))
#else
#define LBUG_API __attribute__((visibility("default")))
#endif
#endif
#endif
