#pragma once
// Client channel setup: credential file lookup, fixed routing headers and
// gRPC runtime lifetime.

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/interceptor.h>

namespace nixgrpc {

// The server rejects an expired client cert during the TLS handshake, which
// gRPC reports only as "Socket closed", so check up front.
auto readValidCert(const std::string & path) -> std::string;
// Must run before the first channel: gRPC reads these once at init.
void enableGrpcTracing();
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

// Stamps fixed headers (x-nix-system from `system=`) on every RPC so a
// balancer routes store queries and uploads like the builds that follow.
class StaticHeaders final : public grpc::experimental::Interceptor {
public:
  using Headers = std::vector<std::pair<std::string, std::string>>;
  explicit StaticHeaders(std::shared_ptr<const Headers> headers) : headers(std::move(headers)) {}
  void Intercept(grpc::experimental::InterceptorBatchMethods * methods) override {
    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      auto * metadata = methods->GetSendInitialMetadata();
      for (const auto & [key, value] : *headers) {
        if (!metadata->contains(key)) {
          metadata->emplace(key, value);
        }
      }
    }
    methods->Proceed();
  }

  class Factory final : public grpc::experimental::ClientInterceptorFactoryInterface {
  public:
    explicit Factory(Headers hdrs) : headers(std::make_shared<const Headers>(std::move(hdrs))) {}
    auto CreateClientInterceptor(grpc::experimental::ClientRpcInfo * /*info*/)
        -> grpc::experimental::Interceptor * override {
      return new StaticHeaders(headers); // NOLINT(cppcoreguidelines-owning-memory): gRPC deletes it
    }
  private:
    std::shared_ptr<const Headers> headers;
  };

private:
  std::shared_ptr<const Headers> headers;
};

} // namespace nixgrpc
