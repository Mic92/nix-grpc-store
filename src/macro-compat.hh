#pragma once
// Force-included into every translation unit (see meson.build).
//
// nix/util/error.hh defines a function-like MakeError macro, which rewrites
// abseil's status_internal::MakeError when a nix header comes before gRPC.
// Parsing gRPC first, with the macro hidden, makes every later include of it
// a no-op, so the order of nix and gRPC headers cannot matter anywhere.

#pragma push_macro("MakeError")
#undef MakeError
#include <grpcpp/grpcpp.h>
#pragma pop_macro("MakeError")
