#pragma once
// `nix-grpc-daemon --stdio`: the Nix worker protocol on stdin/stdout, served
// from the farm. An sshd ForceCommand turns this into an ssh-ng:// builder
// that needs no plugin on the client.

#include <span>

namespace nixgrpc {

auto runStdio(std::span<char *> args) -> int;

} // namespace nixgrpc
