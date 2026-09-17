#pragma once
// Client channel setup: credential file lookup and gRPC runtime lifetime.

#include <string>

namespace nixgrpc {

// The server rejects an expired client cert during the TLS handshake, which
// gRPC reports only as "Socket closed", so check up front.
auto readValidCert(const std::string & path) -> std::string;
// Same lookup order as Nix's ssl-cert-file setting.
auto defaultCaCert() -> std::string;
// Env override, then per-user XDG data dir, then system-wide location.
auto defaultClientCred(const char * envVar, const std::string & fileName) -> std::string;
// gRPC frees TLS endpoints on EventEngine threads that outlive
// grpc_shutdown_blocking(), racing OpenSSL's atexit cleanup. Owning the
// default EventEngine lets us wait for those threads (bounded, nix may exit
// with a store still referenced). Not at plugin load: nix-daemon's forked
// workers must not run this.
void retainGrpcRuntime();

} // namespace nixgrpc
