#include "sandbox.hh"

#include <nix/util/error.hh>

namespace nixgrpc::sandbox {

void apply(const Policy &) { throw nix::Error("--sandbox is not supported on this platform (macOS and Linux only)"); }

} // namespace nixgrpc::sandbox
