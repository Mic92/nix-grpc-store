#pragma once
// Seatbelt profile generation. Pure text, so it is testable on any platform.

#include <string>
#include <vector>

#include "sandbox.hh"

namespace nixgrpc::sandbox::sbpl {

// Paths are referenced as (param "R0") etc. and delivered by parameters(), so
// no path is ever spliced into the profile text.
auto profile(const Policy & policy) -> std::string;
// Flat key, value list for sandbox_init_with_parameters.
auto parameters(const Policy & policy) -> std::vector<std::string>;

} // namespace nixgrpc::sandbox::sbpl
